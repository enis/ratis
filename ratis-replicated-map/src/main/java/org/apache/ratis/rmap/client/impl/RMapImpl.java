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

package org.apache.ratis.rmap.client.impl;

import java.io.IOException;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.rmap.client.Client;
import org.apache.ratis.rmap.client.RMap;
import org.apache.ratis.rmap.common.RMapName;

public class RMapImpl<K, V> implements RMap<K, V> {
  private final RaftClient raftClient;
  private final RMapName rmapName;
  private final Client client;

  RMapImpl(RMapName id, ClientImpl client) {
    this.rmapName = id;
    this.client = client;
    this.raftClient = client.getRaftClient();
  }

  public void put(K key, V value) throws IOException {
    // TODO: generate request and send it via raftClient
  }

  public V get(K key) throws IOException {
    // TODO:
    return null;
  }

  // TODO: checkAndPut, putIfAbsent, etc
  // TODO: iterate

  @Override
  public void close() throws IOException {
  }
}
