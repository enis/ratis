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

package org.apache.ratis.rmap.meta;

import java.io.IOException;

import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.common.RMapName;
import org.apache.ratis.rmap.protocol.ProtobufConverter;
import org.apache.ratis.rmap.protocol.Serde;
import org.apache.ratis.rmap.protocol.Serde.ByteStringSerde;
import org.apache.ratis.rmap.protocol.Serde.StringSerde;
import org.apache.ratis.rmap.statemachine.CreateRMapContext;
import org.apache.ratis.rmap.storage.RMapStore;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.shaded.proto.rmap.RMapProtos;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.CreateRMapWALEntry;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Id;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.WALEntry;

import com.google.common.base.Preconditions;

/**
 * Meta map implements the "schema" of the database. It contains
 * information about other replicated maps. The MetaMap is internally another rmap.
 * Data is organized hierarchically using a filesystem-like naming convention.
 * /rmap/id/info => information about the RMap
 * /id/max  => maximum rmap id used so far
 */
public class MetaMap {
  /**
   * Id of the meta-map.
   */
  public static final long META_MAP_ID = 0;
  public static final String ID_KEY = "/id/max";
  public static final String RMAP_KEY = "/rmap";
  public static final String INFO = "info";

  /**
   * Meta Map's Info object
   */
  public static final RMapInfo<String, ByteString, StringSerde, ByteStringSerde, ?> metaMapInfo =
      RMapInfo.newBuilder()
          .withId(META_MAP_ID)
          .withName(RMapName.valueOf("meta"))
          .withKeyClass(String.class)
          .withValueClass(ByteString.class)
          .withKeySerdeClass(StringSerde.class)
          .withValueSerdeClass(ByteStringSerde.class)
          .build();

  private final RMapStore<String, ByteString, StringSerde, ByteStringSerde, ?> store;
  private final Serde<String> keySerde;
  private final Serde<ByteString> valueSerde;

  public MetaMap() {
    store = new RMapStore<>(metaMapInfo);
    keySerde = store.getKeySerde();
    valueSerde = store.getValueSerde();
    bootstrap(); // TODO: initialize from snapshot
  }

  /**
   * Initialize the Meta map from empty state.
   */
  private void bootstrap() {
    // insert RMapInfo if it is not there
    insertRMap(metaMapInfo);

    // initialize the id
    store.put(ID_KEY, getIdValue(metaMapInfo.getId()));
  }

  public CreateRMapContext preCreateRMap(RMapProtos.RMapInfo info)  throws IOException {
    // TODO: obtain locks for concurrency
    // TODO: make sure that map with same name does not exist

    long id = nextId();

    // clone the RMapInfo, and set the id
    info = RMapProtos.RMapInfo.newBuilder(info)
        .setRmapId(id)
        .build();

    // Build WAL buildEntry for this DDL statement
    WALEntry.Builder builder = WALEntry.newBuilder()
        .setCreateRmapEntry(
            CreateRMapWALEntry.newBuilder()
                .setId(Id.newBuilder().setId(id))
                .setRmapInfo(info))
        .setRmapId(metaMapInfo.getId());

    return new CreateRMapContext(builder.build(), ProtobufConverter.fromProto(info), info);
  }

  public void applyCreateRMap(WALEntry walEntry) {
    CreateRMapWALEntry createRmapEntry = walEntry.getCreateRmapEntry();
    RMapProtos.RMapInfo info = createRmapEntry.getRmapInfo();
    Id id = createRmapEntry.getId();

    ByteString rmapInfoVal = info.toByteString();
    ByteString idVal = id.toByteString();

    store.put(getRMapKey(info.getRmapId()), rmapInfoVal);
    store.put(ID_KEY, idVal);
  }

  private ByteString getIdValue(long id) {
    return Id.newBuilder().setId(id).build().toByteString();
  }

  private long currentId() throws InvalidProtocolBufferException {
    ByteString val = store.get(ID_KEY);
    assert val != null;

    return Id.parseFrom(val).getId();
  }

  private long nextId() throws InvalidProtocolBufferException {
    return currentId() + 1;
  }

  private String getRMapKey(long id) {
    Preconditions.checkArgument(id >= 0);
    return RMAP_KEY + "/" + id + "/" + INFO;
  }

  private void insertRMap(RMapInfo info) {
    RMapProtos.RMapInfo proto = ProtobufConverter.toProto(info);
    store.put(getRMapKey(proto.getRmapId()), proto.toByteString());
  }

  public RMapProtos.RMapInfo getRMap(long id) throws IOException {
    ByteString str = store.get(getRMapKey(id));
    if (str == null) {
      return null;
    }
    return RMapProtos.RMapInfo.parseFrom(str);
  }

  public RMapStore<String, ByteString, StringSerde, ByteStringSerde, ?> getStore() {
    return store;
  }
}
