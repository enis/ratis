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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.RaftClientSenderWithGrpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rmap.common.RMapId;

/**
 * RMapClient maintains the connection to the quorum. Instances of RMaps can be created from
 * the client to read and write data.
 */
public class RMapClient implements Closeable {

  private RaftClient raftClient;

  public RMapClient (String[] servers) {
    this.raftClient = createRaftClient(servers);
  }

  private RaftClient createRaftClient(String[] servers) {
    List<RaftPeer> peers = Arrays.stream(servers).map(addr -> new RaftPeer(addr, addr))
        .collect(Collectors.toList());
    RaftClientSenderWithGrpc requestSender = new RaftClientSenderWithGrpc(peers);
    RaftProperties properties = new RaftProperties();
    return RaftClient.newBuilder()
        .setProperties(properties)
        .setRequestSender(requestSender)
        .setServers(peers)
        .build();
  }

  public boolean createRMap(RMapId id) {
    // TODO: create a new RMap with the given id in the cluster
    return false;
  }

  public boolean deleteRMap(RMapId id) {
    // TODO
    return false;
  }

  public List<RMapId> listRMapIds() {
    // TODO
    return null;
  }

  /**
   * Creates and returns an RMap instance to access the map data.
   * @param id
   * @param <K>
   * @param <V>
   * @return
   */
  public <K,V> RMap<K,V> getRMap(RMapId id) {
    return new RMap<K, V>(id, this);
  }

  @Override
  public void close() throws IOException {
    this.raftClient.close();
  }
}
