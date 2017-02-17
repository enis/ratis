/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ratis.rmap.statemachine;


import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.rmap.meta.MetaMap;
import org.apache.ratis.rmap.protocol.ProtobufConverter;
import org.apache.ratis.rmap.storage.RMapStore;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.TextFormat;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.shaded.proto.rmap.RMapProtos;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Entry;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.WALEntry;
import org.apache.ratis.statemachine.BaseStateMachine;
import org.apache.ratis.statemachine.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMapStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(RMapStateMachine.class);

  private final MetaMap metaMap;
  private final SimpleStateMachineStorage storage;
  private final ConcurrentHashMap<Long, RMapStore> rmaps = new ConcurrentHashMap<>();

  public RMapStateMachine() {
    this.storage = new SimpleStateMachineStorage();
    this.metaMap = new MetaMap();
    this.rmaps.put(MetaMap.metaMapInfo.getId(), metaMap.getStore());
  }

  @Override
  public void initialize(String id, RaftProperties properties, RaftStorage raftStorage)
      throws IOException {
    super.initialize(id, properties, raftStorage);
    this.storage.init(raftStorage);
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    RMapProtos.Request req = ProtobufConverter.parseRequestFrom(request.getMessage().getContent());
    if (LOG.isTraceEnabled()) {
      LOG.trace("starting transaction: {}", TextFormat.shortDebugString(req));
    }
    TransactionContext transaction = super.startTransaction(request);

    switch (req.getRequestTypeCase()) {
      case CREATE_RMAP_REQUEST:
        return startCreateRmapTransaction(req.getCreateRmapRequest(), transaction);
      case DELETE_RMAP_REQUEST:
        break;
      case LIST_RMAP_INFOS_REQUEST:
        break;
      case MULTI_ACTION_REQUEST:
        break;
      case REQUESTTYPE_NOT_SET:
        throw new IOException("Request type is not set");
      default:
        throw new IOException("Unknown request type");
    }
    return transaction;
  }

  private TransactionContext startCreateRmapTransaction(RMapProtos.CreateRMapRequest req,
                                                        TransactionContext transaction)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("starting CREATE RMAP transaction: {}", TextFormat.shortDebugString(req));
    }
    WALEntry walEntry = metaMap.preCreateRMap(req.getRmapInfo());
    transaction.setSmLogEntryProto(SMLogEntryProto.newBuilder()
        .setData(walEntry.toByteString())
        .build());
    return transaction;
  }

  @Override
  public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
    return super.preAppendTransaction(trx);
  }

  @Override
  public TransactionContext applyTransactionSerial(TransactionContext trx) throws IOException {
    return super.applyTransactionSerial(trx);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) throws IOException {
    ByteString data = trx.getSMLogEntry().get().getData();
    WALEntry walEntry = WALEntry.parseFrom(data);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Applying WALEntry:{}", TextFormat.shortDebugString(walEntry)); // TODO: trace
    }

    long rmapId = walEntry.getRmapId();
    RMapStore store = rmaps.get(rmapId);

    if (store == null) {
      throw new IOException("Cannot find RMap with id:" + rmapId + ", for transaction: " + trx);
    }

    // TODO: locks and all

    for (Entry entry : walEntry.getEntryList()) {
      ByteString key = entry.getKey();
      ByteString value = entry.getValue();
      store.put(key, value);
    }

    LOG.info(store.debugDump());

    return super.applyTransaction(trx);
  }
}
