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

package org.apache.ratis;

import java.io.IOException;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGRpc;
import org.apache.ratis.rmap.statemachine.RMapStateMachine;

import com.google.common.base.Preconditions;

public class MiniRMapCluster {

  public static class Builder {
    RaftProperties properties;
    int numServers = -1;

    public Builder withRaftProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }

    public Builder withNumServers(int numServers) {
      this.numServers = numServers;
      return this;
    }

    public MiniRMapCluster build() throws IOException {
      Preconditions.checkNotNull(numServers > 0);
      if (properties == null) {
        properties = new RaftProperties();
      }
      properties.set(MiniRaftCluster.STATEMACHINE_CLASS_KEY, RMapStateMachine.class.getName());

      return new MiniRMapCluster(
          MiniRaftClusterWithGRpc.FACTORY.newCluster(numServers, properties));
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  protected final MiniRaftClusterWithGRpc miniCluster;

  public MiniRMapCluster(MiniRaftClusterWithGRpc miniCluster) {
    this.miniCluster = miniCluster;
  }

  public void start() {
    miniCluster.start();
  }

  public void shutdown() {
    miniCluster.shutdown();
  }

  public MiniRaftClusterWithGRpc getMiniCluster() {
    return miniCluster;
  }
}
