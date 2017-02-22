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

import org.apache.ratis.rmap.protocol.Serde;

import com.google.common.base.Preconditions;

/**
 * Metadata about a replicated map.
 */
public class RMapInfo<
    K,
    V,
    KS extends Serde<K>,
    VS extends Serde<V>,
    KC extends Comparator<? super K>> {

  /**
   * Indicates an rmap id which is not set yet. Only Rmap's which are not yet created
   * have their rmap ids unset.
   */
  public static final long UNSET_RMAP_ID = -1;

  private final long id;
  private final RMapName name;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Class<KS> keySerdeClass;
  private final Class<VS> valueSerdeClass;
  private final Class<KC> keyComparatorClass;

  public static class Builder<
      K,
      V,
      KS extends Serde<K>,
      VS extends Serde<V>,
      KC extends Comparator<? super K>> {
    private RMapName name;
    private long id = UNSET_RMAP_ID;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private Class<KC> keyComparatorClass;
    private Class<KS> keySerdeClass;
    private Class<VS> valueSerdeClass;

    public Builder withRMapInfo(RMapInfo<K,V,KS,VS,KC> info) {
      this.id = info.id;
      this.name = info.name;
      this.keyClass = info.keyClass;
      this.valueClass = info.valueClass;
      this.keySerdeClass = info.keySerdeClass;
      this.valueSerdeClass = info.valueSerdeClass;
      this.keyComparatorClass = info.keyComparatorClass;
      return this;
    }

    public Builder withName(RMapName name) {
      this.name = name;
      return this;
    }

    public Builder withId(long id) {
      this.id = id;
      return this;
    }

    public Builder withKeyClass(Class<K> keyClass) {
      this.keyClass = keyClass;
      return this;
    }

    public Builder withValueClass(Class<V> valueClass) {
      this.valueClass = valueClass;
      return this;
    }

    public Builder withKeySerdeClass(Class<KS> keySerdeClass) {
      this.keySerdeClass = keySerdeClass;
      return this;
    }

    public Builder withValueSerdeClass(Class<VS> valueSerdeClass) {
      this.valueSerdeClass = valueSerdeClass;
      return this;
    }

    public Builder withKeyComparatorClass(Class<KC> keyComparatorClass) {
      this.keyComparatorClass = keyComparatorClass;
      return this;
    }

    public RMapInfo build() {
      return new RMapInfo(id, name, keyClass, valueClass,
          keySerdeClass, valueSerdeClass, keyComparatorClass);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private RMapInfo(long id, RMapName name, Class<K> keyClass, Class<V> valueClass,
                   Class<KS> keySerdeClass, Class<VS> valueSerdeClass,
                   Class<KC> keyComparatorClass) {
    Preconditions.checkNotNull(this.id = id);
    Preconditions.checkNotNull(this.name = name);
    Preconditions.checkNotNull(this.keyClass = keyClass);
    Preconditions.checkNotNull(this.valueClass = valueClass);
    Preconditions.checkNotNull(this.keySerdeClass = keySerdeClass);
    Preconditions.checkNotNull(this.valueSerdeClass = valueSerdeClass);
    this.keyComparatorClass = keyComparatorClass; // can be null

    if (keyComparatorClass == null) {
      if (!Comparable.class.isAssignableFrom(keyClass)) {
        throw new IllegalArgumentException("Provided keyClass: " + keyClass + " should implement" +
            " Comparable OR a Comparator class sholud be given for keys.");
      }
    }
  }

  public long getId() {
    return id;
  }

  public RMapName getName() {
    return name;
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public Class<V> getValueClass() {
    return valueClass;
  }

  public Class<KS> getKeySerdeClass() {
    return keySerdeClass;
  }

  public Class<VS> getValueSerdeClass() {
    return valueSerdeClass;
  }

  public Class<KC> getKeyComparatorClass() {
    return keyComparatorClass;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("RMapInfo{ id:")
        .append(id)
        .append(" ,name:")
        .append(name)
        .append(" ,keyClass:")
        .append(keyClass.getName())
        .append(" ,valueClass:")
        .append(valueClass.getName())
        .append(" ,keySerdeClass:")
        .append(keySerdeClass.getName())
        .append(" ,valueSerdeClass:")
        .append(valueSerdeClass.getName())
        .append(" ,keyComparatorClass:")
        .append(keyComparatorClass == null ? "null" : keyComparatorClass.getName())
        .append(" }")
        .toString();
  }
}
