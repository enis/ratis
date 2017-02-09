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

package org.apache.ratis.rmap.common;

import java.util.Comparator;

import com.google.common.base.Preconditions;

/**
 * Metadata about a replicated map.
 */
public class RMapInfo<K, V, KC extends Comparator<? super K>> {
  private final RMapId id;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Class<KC> keyComparator;

  // TODO: KeySerialiser and ValueSerializer classes ?

  public static class Builder<K, V, KC extends Comparator<? super K>> {
    private RMapId id;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private Class<KC> keyComparator;

    Builder withId(RMapId id) {
      this.id = id;
      return this;
    }

    Builder withKeyClass(Class<K> keyClass) {
      this.keyClass = keyClass;
      return this;
    }

    Builder withValueClass(Class<V> valueClass) {
      this.valueClass = valueClass;
      return this;
    }

    Builder withKeyComparator(Class<KC> keyComparator) {
      this.keyComparator = keyComparator;
      return this;
    }

    RMapInfo build() {
      return new RMapInfo(id, keyClass, valueClass, keyComparator);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private RMapInfo(RMapId id, Class<K> keyClass, Class<V> valueClass, Class<KC> keyComparator) {
    Preconditions.checkNotNull(this.id = id);
    Preconditions.checkNotNull(this.keyClass = keyClass);
    Preconditions.checkNotNull(this.valueClass = valueClass);
    this.keyComparator = keyComparator; // can be null

    if (keyComparator == null) {
      if (!Comparable.class.isAssignableFrom(keyClass)) {
        throw new IllegalArgumentException("Provided keyClass: " + keyClass + " should implement" +
            " Comparable OR a Comparator class sholud be given for keys.");
      }
    }
  }

  public RMapId getId() {
    return id;
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public Class<V> getValueClass() {
    return valueClass;
  }

  public Class<KC> getKeyComparator() {
    return keyComparator;
  }
}
