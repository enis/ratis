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

import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.common.RMapName;
import org.apache.ratis.rmap.protocol.ProtobufConverter;
import org.apache.ratis.rmap.protocol.Serde;
import org.apache.ratis.rmap.protocol.Serde.ByteStringSerde;
import org.apache.ratis.rmap.protocol.Serde.StringSerde;
import org.apache.ratis.rmap.storage.RMapStore;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.shaded.proto.rmap.RMapProtos;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Entry;
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

  private Entry entry(String key, ByteString value) {
    return Entry.newBuilder()
        .setKey(keySerde.serialize(key))
        .setValue(valueSerde.serialize(value))
        .build();
  }

  public RMapProtos.WALEntry preCreateRMap(RMapProtos.RMapInfo info)
      throws InvalidProtocolBufferException {
    // TODO: obtain locks for concurrency
    // TODO: make sure that map with same name does not exist

    long id = nextId();

    // clone the RMapInfo, and set the id
    info = RMapProtos.RMapInfo.newBuilder(info)
        .setRmapId(id)
        .build();

    ByteString rmapInfoVal = info.toByteString();
    ByteString idVal = Id.newBuilder().setId(id).build().toByteString();

    // Build WAL entry for this DDL statement
    WALEntry.Builder builder = WALEntry.newBuilder()
        .addEntry(entry(getRMapKey(info), rmapInfoVal))
        .addEntry(entry(ID_KEY, idVal))
        .setRmapId(metaMapInfo.getId());

    return builder.build();
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

  private String getRMapKey(RMapProtos.RMapInfo info) {
    Preconditions.checkArgument(info.getRmapId() >= 0);
    return RMAP_KEY + "/" + info.getRmapId() + "/" + INFO;
  }

  private void insertRMap(RMapInfo info) {
    RMapProtos.RMapInfo proto = ProtobufConverter.toProto(info);
    store.put(getRMapKey(proto), proto.toByteString());
  }

  public RMapStore<String, ByteString, StringSerde, ByteStringSerde, ?> getStore() {
    return store;
  }
}
