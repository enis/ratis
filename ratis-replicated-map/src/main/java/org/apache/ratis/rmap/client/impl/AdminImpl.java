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
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.rmap.client.Admin;
import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.protocol.ProtobufConverter;
import org.apache.ratis.rmap.protocol.Request;

class AdminImpl implements Admin {

  private final ClientImpl client;
  private final RaftClient raftClient;

  AdminImpl(ClientImpl client) {
    this.client = client;
    this.raftClient = client.getRaftClient();
  }

  public boolean createRMap(RMapInfo info) throws IOException {
    Request req = ProtobufConverter.buildCreateRMapRequest(info);
    RaftClientReply response = raftClient.send(req);

    return false;
  }

  @Override
  public boolean deleteRMap(long rmapId) {
    return false;
  }

  @Override
  public List<RMapInfo> listRMapInfos() {
    return null;
  }

  @Override
  public void close() throws IOException {
  }
}