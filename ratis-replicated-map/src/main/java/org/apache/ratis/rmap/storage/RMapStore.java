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

package org.apache.ratis.rmap.storage;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.protocol.Serde;
import org.apache.ratis.rmap.util.ReflectionUtils;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Entry;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.PutRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.WALEntry;

/**
 * This is the in-memory implementation for a sorted map of K to V. The concurrency model
 * is pretty simple for now, and does not pipeline the memory insertions with the WAL commits.
 * @param <K> the class for keys
 * @param <V> the class for values
 */
public class RMapStore<
    K,
    V,
    KS extends Serde<K>,
    VS extends Serde<V>,
    KC extends Comparator<? super K>> {
  private final RMapInfo<K, V, KS, VS, KC> info;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;

  private final ConcurrentSkipListMap<K, V> map;

  public RMapStore(RMapInfo info) {
    this.info = info;
    this.keySerde = ReflectionUtils.<Serde<K>>newInstance(info.getKeySerdeClass());
    this.valueSerde = ReflectionUtils.<Serde<V>>newInstance(info.getValueSerdeClass());

    if (info.getKeyComparatorClass() != null) {
      Comparator<? super K> comparator
          = (Comparator<? super K>) ReflectionUtils.newInstance(info.getKeyComparatorClass());
      this.map = new ConcurrentSkipListMap<>(comparator);
    } else {
      this.map = new ConcurrentSkipListMap<>();
    }
  }

  public Serde<K> getKeySerde() {
    return keySerde;
  }

  public Serde<V> getValueSerde() {
    return valueSerde;
  }

  public Entry buildEntry(K key, V value) {
    return Entry.newBuilder()
        .setKey(keySerde.serialize(key))
        .setValue(valueSerde.serialize(value))
        .build();
  }

  private Entry buildEntry(ByteString key, ByteString value) {
    return Entry.newBuilder()
        .setKey(key)
        .setValue(value)
        .build();
  }

  public WALEntry buildWALEntry(List<PutRequest> actions) {
    WALEntry.Builder builder = WALEntry.newBuilder()
        .setRmapId(info.getId());
    for (PutRequest action : actions) {
      builder.addEntry(buildEntry(action.getKey(), action.getValue()));
    }

    return builder.build();
  }

  public void put(K key, V value) {
    map.put(key, value);
  }

  public void put(ByteString key, ByteString value) {
    K k = keySerde.deserialize(key);
    V v = valueSerde.deserialize(value);
    put(k,v);
  }

  public V get(K key) {
    return map.get(key);
  }

  public ByteString getAsByteString(ByteString key) {
    V v = get(keySerde.deserialize(key));
    if (v == null) {
      return null;
    }
    return valueSerde.serialize(v);
  }

  public String debugDump() {
    StringBuilder builder = new StringBuilder();
    builder.append("RMapStore{ rmap_info:")
        .append(info)
        .append(", map:")
        .append(map);
    return builder.toString();
  }
}
