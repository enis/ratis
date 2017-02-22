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

package org.apache.ratis.rmap.statemachine;

import org.apache.ratis.shaded.proto.rmap.RMapProtos.RMapInfo;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.WALEntry;

public class CreateRMapContext extends WriteContext {
  private final RMapInfo protoRMapInfo;
  private final org.apache.ratis.rmap.common.RMapInfo rMapInfo;

  public CreateRMapContext(WALEntry walEntry, org.apache.ratis.rmap.common.RMapInfo rMapInfo,
                           RMapInfo protoRMapInfo) {
    super(Type.CREATE_RMAP, walEntry);
    this.rMapInfo = rMapInfo;
    this.protoRMapInfo = protoRMapInfo;
  }

  public org.apache.ratis.rmap.common.RMapInfo getRMapInfo() {
    return rMapInfo;
  }

  public RMapInfo getProtoRMapInfo() {
    return protoRMapInfo;
  }

}
