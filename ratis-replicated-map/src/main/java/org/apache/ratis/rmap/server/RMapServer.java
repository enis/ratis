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

package org.apache.ratis.rmap.server;

import static org.apache.ratis.grpc.RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_KEY;
import static org.apache.ratis.server.RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RpcType;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rmap.common.FileQuorumSupplier;
import org.apache.ratis.rmap.common.QuorumSupplier;
import org.apache.ratis.rmap.statemachine.RMapStateMachine;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import com.google.common.base.Preconditions;

/**
 * RMap server
 */
public class RMapServer {
  private final int port;
  private RaftServer raftServer;
  private RaftProperties properties;

  private RMapServer(String id, QuorumSupplier quorumSupplier) throws IOException {
    properties = new RaftProperties();
    properties.setBoolean(RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY, false);

    List<RaftPeer> peers = quorumSupplier.getQuorum();
    RaftPeerId myId = RaftPeerId.getRaftPeerId(id);

    Preconditions.checkArgument(peers.stream().anyMatch(p -> p.getId().equals(myId)),
        "%s is not one of %s specified in %s", id,
        peers.stream().map(RaftPeer::getId).collect(Collectors.toList()),
        FileQuorumSupplier.FILENAME);

    this.port = NetUtils.createSocketAddr(id).getPort();

    String idForPath = URLEncoder.encode(id, "UTF-8");

    properties.set(RAFT_SERVER_STORAGE_DIR_KEY,
        "/tmp/rmap-server-" + idForPath);
    properties.setInt(RAFT_GRPC_SERVER_PORT_KEY, port);

    RaftConfigKeys.Rpc.setType(properties::setEnum, RpcType.GRPC);

    raftServer = RaftServer.newBuilder()
        .setServerId(myId)
        .setPeers(peers)
        .setProperties(properties)
        .setStateMachine(new RMapStateMachine())
        .build();
  }

  public void start() {
    raftServer.start();
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: RMapServer <id>");
      System.exit(1);
    }

    String id = args[0];

    RMapServer server = new RMapServer(id, new FileQuorumSupplier());
    server.start();
  }
}
