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

package org.apache.ratis.rmap.client;

public class Scan<K> {
  private K startKey;
  private K endKey;
  private boolean startKeyInclusive;
  private boolean endKeyInclusive;
  private boolean keysOnly;
  private int limit;

  public Scan() {
    startKey = null;
    endKey = null;
    startKeyInclusive = true;
    endKeyInclusive = false;
    keysOnly = false;
    limit = 0;
  }

  public Scan<K> setStartKey(K startKey) {
    this.startKey = startKey;
    return this;
  }

  public Scan<K> setEndKey(K endKey) {
    this.endKey = endKey;
    return this;
  }

  public Scan<K> setStartKeyInclusive(boolean startKeyInclusive) {
    this.startKeyInclusive = startKeyInclusive;
    return this;
  }

  public Scan<K> setEndKeyInclusive(boolean endKeyInclusive) {
    this.endKeyInclusive = endKeyInclusive;
    return this;
  }

  public Scan<K> setKeysOnly(boolean keysOnly) {
    this.keysOnly = keysOnly;
    return this;
  }

  public Scan<K> setLimit(int limit) {
    this.limit = limit;
    return this;
  }

  public K getStartKey() {
    return startKey;
  }

  public K getEndKey() {
    return endKey;
  }

  public boolean isStartKeyInclusive() {
    return startKeyInclusive;
  }

  public boolean isEndKeyInclusive() {
    return endKeyInclusive;
  }

  public boolean isKeysOnly() {
    return keysOnly;
  }

  public int getLimit() {
    return limit;
  }
}
