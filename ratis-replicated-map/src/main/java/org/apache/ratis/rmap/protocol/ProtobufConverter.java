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

package org.apache.ratis.rmap.protocol;

import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.shaded.proto.rmap.RMapProtos;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.CreateRMapRequest;

public class ProtobufConverter {
  public static RMapProtos.RMapInfo toProto(RMapInfo rMapInfo) {
    RMapProtos.RMapInfo.Builder builder = RMapProtos.RMapInfo.newBuilder()
        .setRmapId(rMapInfo.getId())
        .setName(rMapInfo.getName().toString())
        .setKeyClass(rMapInfo.getKeyClass().getName())
        .setValueClass(rMapInfo.getValueClass().getName());

    if (rMapInfo.getKeyComparatorClass() != null) {
      builder.setKeyComparator(rMapInfo.getKeyComparatorClass().getName());
    }

    return builder.build();
  }

  public static Request buildCreateRMapRequest(RMapInfo rMapInfo) {
    CreateRMapRequest req = CreateRMapRequest.newBuilder()
        .setRmapInfo(toProto(rMapInfo))
        .build();

    return new Request(
        RMapProtos.Request.newBuilder()
        .setCreateRmapRequest(req)
        .build()
    );
  }

  public static RMapProtos.Request parseRequestFrom(ByteString buf)
      throws InvalidProtocolBufferException {
    return RMapProtos.Request.parseFrom(buf);
  }
}
