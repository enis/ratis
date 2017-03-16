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

package org.apache.ratis.rmap.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Searches for rmap-quorum file in the classpath and reads the peer servers. Every server should
 * be in a separate line.
 */
public class FileQuorumSupplier implements QuorumSupplier {
  public static final String FILENAME = "rmap-quorum";

  private List<RaftPeer> peers;

  public FileQuorumSupplier() throws IOException {
    List<String> servers = readFile();
    peers = servers.stream().map(
        addr -> new RaftPeer(RaftPeerId.getRaftPeerId(addr), addr))
        .collect(Collectors.toList());
  }

  private static List<String> readFile() throws IOException {
    List<String> lines = new ArrayList<>();
    try (InputStream in = FileQuorumSupplier.class.getClassLoader()
        .getResourceAsStream(FILENAME);
         BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("#")) {
          // skip
        } else {
          lines.add(line);
        }
      }
    }
    return lines;
  }

  @Override
  public List<RaftPeer> getQuorum() {
    return peers;
  }
}
