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
import java.util.List;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.RaftClientSenderWithGrpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rmap.client.Admin;
import org.apache.ratis.rmap.client.Client;
import org.apache.ratis.rmap.client.RMap;
import org.apache.ratis.rmap.common.QuorumSupplier;
import org.apache.ratis.rmap.common.RMapInfo;

public class ClientImpl implements Client {
  private RaftClient raftClient;

  public ClientImpl(QuorumSupplier quorumSupplier) {
    this.raftClient = createRaftClient(quorumSupplier);
  }

  private RaftClient createRaftClient(QuorumSupplier quorumSupplier) {
    List<RaftPeer> peers = quorumSupplier.getQuorum();
    RaftClientSenderWithGrpc requestSender = new RaftClientSenderWithGrpc(peers);
    RaftProperties properties = new RaftProperties();
    return RaftClient.newBuilder()
        .setProperties(properties)
        .setRequestSender(requestSender)
        .setServers(peers)
        .build();
  }

  RaftClient getRaftClient() {
    return raftClient;
  }

  @Override
  public Admin getAdmin() {
    return new AdminImpl(this);
  }

  /**
   * Creates and returns an RMap instance to access the map data.
   * @param id
   * @param <K>
   * @param <V>
   * @return
   */
  @Override
  public <K,V> RMap<K,V> getRMap(long id) throws IOException {
    try (Admin admin = getAdmin()) {
      RMapInfo info = admin.getRmapInfo(id);
      return new RMapImpl<K, V>(info, this);
    }
  }

  @Override
  public void close() throws IOException {
    this.raftClient.close();
  }
}
