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
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.util.ReflectionUtils;

/**
 * This is the in-memory implementation for a sorted map of K to V. The concurrency model
 * is pretty simple for now, and does not pipeline the memory insertions with the WAL commits.
 * @param <K> the class for keys
 * @param <V> the class for values
 */
public class RMapStore<K, V> {
  private final RMapInfo info;

  private final ConcurrentSkipListMap<K, V> map;

  public RMapStore(RMapInfo info) {
    this.info = info;

    if (info.getKeyComparator() != null) {
      Comparator<? super K> comparator
          = (Comparator<? super K>) ReflectionUtils.newInstance(info.getKeyComparator());
      this.map = new ConcurrentSkipListMap<>(comparator);
    } else {
      this.map = new ConcurrentSkipListMap<>();
    }
  }

  public void put(K key, V value) {
    map.put(key, value);
  }

  public V get(K key) {
    return map.get(key);
  }
}
