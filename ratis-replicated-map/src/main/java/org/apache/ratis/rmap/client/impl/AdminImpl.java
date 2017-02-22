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
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.rmap.client.Admin;
import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.common.RMapNotFoundException;
import org.apache.ratis.rmap.protocol.ProtobufConverter;
import org.apache.ratis.rmap.protocol.Request;
import org.apache.ratis.shaded.proto.rmap.RMapProtos;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.CreateRMapResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.ListRMapInfosResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AdminImpl implements Admin {
  static final Logger LOG = LoggerFactory.getLogger(AdminImpl.class);

  private final ClientImpl client;
  private final RaftClient raftClient;

  AdminImpl(ClientImpl client) {
    this.client = client;
    this.raftClient = client.getRaftClient();
  }

  public RMapInfo createRMap(RMapInfo info) throws IOException {
    Request req = ProtobufConverter.buildCreateRMapRequest(info);
    RaftClientReply reply = raftClient.send(req);

    if (!reply.isSuccess()) {
      throw new IOException("Request failed :" + reply);
    }

    Response resp = ProtobufConverter.parseResponseFrom(reply.getMessage().getContent());
    CreateRMapResponse createRMapResponse = resp.getCreateRmapResponse();
    assert createRMapResponse != null;

    RMapInfo ret = ProtobufConverter.fromProto(createRMapResponse.getRmapInfo());
    if (!ret.getName().equals(info.getName())) {
      throw new IOException("Coding error! Server returned an RMapInfo object for a different " +
          "rmap. Request:" + info + " ,Response:" + ret);
    }
    return ret;
  }

  @Override
  public boolean deleteRMap(long rmapId) {
    return false;
  }

  @Override
  public RMapInfo getRmapInfo(long rMapId) throws IOException {
    List<RMapInfo> infos = listRMapInfos(rMapId, null);
    if (infos.isEmpty()) {
      throw new RMapNotFoundException(rMapId);
    }
    return infos.get(0);
  }

  @Override
  public List<RMapInfo> listRMapInfos() throws IOException {
    return listRMapInfos(null);
  }

  @Override
  public List<RMapInfo> listRMapInfos(String namePattern) throws IOException {
    return listRMapInfos(RMapInfo.UNSET_RMAP_ID, namePattern);
  }

  private List<RMapInfo> listRMapInfos(long rMapId, String namePattern) throws IOException {
    Request req = ProtobufConverter.buildListRMapInfosRequest(rMapId, namePattern);
    RaftClientReply reply = raftClient.sendReadOnly(req);

    if (!reply.isSuccess()) {
      throw new IOException("Request failed :" + reply);
    }

    Response resp = ProtobufConverter.parseResponseFrom(reply.getMessage().getContent());
    ListRMapInfosResponse listRMapInfosResponse = resp.getListRmapInfosResponse();
    assert listRMapInfosResponse != null;

    ArrayList<RMapInfo> ret = new ArrayList<>(listRMapInfosResponse.getRmapInfoCount());
    for (RMapProtos.RMapInfo info : listRMapInfosResponse.getRmapInfoList()) {
      ret.add(ProtobufConverter.fromProto(info));
    }
    return ret;
  }

  @Override
  public void close() throws IOException {
  }
}