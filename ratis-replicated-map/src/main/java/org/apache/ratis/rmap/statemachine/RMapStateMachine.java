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


import static org.apache.ratis.rmap.meta.MetaMap.metaMapInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rmap.common.RMapNotFoundException;
import org.apache.ratis.rmap.meta.MetaMap;
import org.apache.ratis.rmap.protocol.ProtobufConverter;
import org.apache.ratis.rmap.protocol.Response;
import org.apache.ratis.rmap.storage.RMapStore;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.TextFormat;
import org.apache.ratis.shaded.proto.rmap.RMapProtos;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Action;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Action.ActionTypeCase;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.CreateRMapWALEntry;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Entry;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.ListRMapInfosRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.MultiActionRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.PutRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.RMapInfo;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.ScanRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.WALEntry;
import org.apache.ratis.statemachine.BaseStateMachine;
import org.apache.ratis.statemachine.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class RMapStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(RMapStateMachine.class);

  private final MetaMap metaMap;
  private final SimpleStateMachineStorage storage;
  private final ConcurrentHashMap<Long, RMapStore> rmaps = new ConcurrentHashMap<>();

  public RMapStateMachine() {
    this.storage = new SimpleStateMachineStorage();
    this.metaMap = new MetaMap();
    this.rmaps.put(metaMapInfo.getId(), metaMap.getStore());
  }

  @Override
  public void initialize(RaftPeerId id, RaftProperties properties, RaftStorage raftStorage)
      throws IOException {
    super.initialize(id, properties, raftStorage);
    this.storage.init(raftStorage);
  }

  @Override
  public CompletableFuture<RaftClientReply> query(RaftClientRequest request) {
    RMapProtos.Request req = null;
    Response response = null;
    try {
      req = ProtobufConverter.parseRequestFrom(request.getMessage().getContent());
      switch (req.getRequestTypeCase()) {
        case LIST_RMAP_INFOS_REQUEST:
          response = listRMapInfos(req.getListRmapInfosRequest());
          break;
        case MULTI_ACTION_REQUEST:
          response = multiGet(req.getMultiActionRequest());
          break;
        case SCAN_REQUEST:
          response = scan(req.getScanRequest());
          break;
        default:
          throw new IOException("Request type is not correct:" + req.getRequestTypeCase());
      }
    } catch (IOException e) {
      // TODO: Add generic exceptions to Ratis responses
      LOG.error("Exception in query", e);
    }

    if (response != null) {
      return CompletableFuture.completedFuture(new RaftClientReply(request, response));
    } else {
      return null;
    }
  }

  private Response listRMapInfos(ListRMapInfosRequest req) throws IOException {
    switch (req.getListRMapInfosTypeCase()) {
      case RMAP_ID:
        RMapInfo info = metaMap.getRMap(req.getRmapId());
        if (info == null) {
          throw new RMapNotFoundException(req.getRmapId());
        }
        return ProtobufConverter.buildListRMapInfosResponse(Lists.newArrayList(info));
      case NAME_PATTERN:
        List<RMapInfo> list = metaMap.listRMaps(req.getNamePattern());
        return ProtobufConverter.buildListRMapInfosResponse(list);
      default:
        throw new IOException("Unknown request type");
    }
  }

  private Response multiGet(MultiActionRequest request) throws IOException {
    RMapStore store = getRMapStore(request.getRmapId());

    List<Entry> list = new ArrayList<>(request.getActionCount());
    for (Action action : request.getActionList()) {
      ByteString key = action.getGetRequest().getKey();
      ByteString value = store.getAsByteString(key);
      Entry.Builder builder = Entry.newBuilder().setKey(key);
      if (value != null) {
        builder.setValue(value);
      }
      list.add(builder.build());
    }
    return ProtobufConverter.buildMultiGetResponse(list.stream());
  }

  private Response scan(ScanRequest request) throws IOException {
    RMapStore store = getRMapStore(request.getRmapId());

    Iterable<Map.Entry> it = store.scan(request.getScan());
    return ProtobufConverter.buildScanResponse(it, store.getKeySerde(), store.getValueSerde(),
        request.getScan().getKeysOnly());
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    RMapProtos.Request req = ProtobufConverter.parseRequestFrom(request.getMessage().getContent());
    TransactionContext transaction = super.startTransaction(request);

    if (LOG.isDebugEnabled()) { // TODO: trace
      LOG.debug("Starting transaction: {}", TextFormat.shortDebugString(req));
    }

    switch (req.getRequestTypeCase()) {
      case CREATE_RMAP_REQUEST:
        return startCreateRMap(req.getCreateRmapRequest(), transaction);
      case DELETE_RMAP_REQUEST:
        break;
      case MULTI_ACTION_REQUEST:
        return startMultiAction(req.getMultiActionRequest(), transaction);
      default:
        throw new IOException("Request type is not correct:" + req.getRequestTypeCase());
    }
    return transaction;
  }

  private TransactionContext startCreateRMap(RMapProtos.CreateRMapRequest req,
                                             TransactionContext transaction)
      throws IOException {
    CreateRMapContext context = metaMap.preCreateRMap(req.getRmapInfo());

    setStateMachineContext(transaction, context);
    return transaction;
  }

  private void setStateMachineContext(TransactionContext transaction, WriteContext context) {
    transaction.setSmLogEntryProto(context.buildSMLogEntry());
    transaction.setStateMachineContext(context);
  }

  private TransactionContext startMultiAction(RMapProtos.MultiActionRequest req,
                                              TransactionContext transaction)
      throws IOException {

    List<Action> actions = req.getActionList();
    for (Action action : actions) {
      // Requests cannot be mixed and matched in a single batch.
      assert action.getActionTypeCase() == ActionTypeCase.PUT_REQUEST;
    }

    List<PutRequest> puts
        = actions.stream().map(Action::getPutRequest).collect(Collectors.toList());

    RMapStore store = getRMapStore(req.getRmapId());

    WALEntry walEntry = store.buildWALEntry(puts);
    MultiActionContext context = new MultiActionContext(walEntry);
    setStateMachineContext(transaction, context);

    return transaction;
  }

  private RMapStore getRMapStore(long id) throws RMapNotFoundException {
    RMapStore store = rmaps.get(id);
    if (store == null) {
      throw new RMapNotFoundException(id);
    }
    return store;
  }

  @Override
  public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
    return super.preAppendTransaction(trx);
  }

  @Override
  public TransactionContext applyTransactionSerial(TransactionContext trx) throws IOException {
    // applyTransactionSerial is by design single threaded, and will guarantee that this is called
    // in the exact same order that the transactions are committed in the raft log. We should be
    // doing parallel inserts by deferring the actual work to applyTransaction() call once we have
    // MVCC-like visibility for write-write and read-write conflicts

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

    switch (walEntry.getWALEntryTypeCase()) {
      case CREATE_RMAP_ENTRY:
        trx.setStateMachineContext(applyCreateRMap(trx, walEntry));
        break;
      case WALENTRYTYPE_NOT_SET: // Multi action
        trx.setStateMachineContext(applyToStore(walEntry, store));
        break;
    }

    LOG.info(store.debugDump()); // TODO: remove

    return super.applyTransactionSerial(trx);
  }

  private Response applyCreateRMap(TransactionContext trx, WALEntry walEntry) throws IOException {
    metaMap.applyCreateRMap(walEntry);

    CreateRMapWALEntry createRmapEntry = walEntry.getCreateRmapEntry();
    RMapProtos.RMapInfo info = createRmapEntry.getRmapInfo();

    // initiate the RMap store
    RMapStore store = new RMapStore(ProtobufConverter.fromProto(info));

    // "open" the RMap for reading and writing
    rmaps.put(info.getRmapId(), store);

    LOG.info("Applied CREATE RMAP with info:{}", TextFormat.shortDebugString(info));

    return ProtobufConverter.buildCreateRMapResponse(info);
  }

  private Response applyToStore(WALEntry walEntry, RMapStore store) {
    for (Entry entry : walEntry.getEntryList()) {
      ByteString key = entry.getKey();
      ByteString value = entry.getValue();
      store.put(key, value);
    }
    return ProtobufConverter.buildMultiPutResponse(Stream.of(walEntry));
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) throws IOException {
    if (trx.getStateMachineContext().isPresent()) {
      Response response = (Response) trx.getStateMachineContext().get();
      return CompletableFuture.completedFuture(response);
    }
    return null; // followers do not need to return response
  }
}
