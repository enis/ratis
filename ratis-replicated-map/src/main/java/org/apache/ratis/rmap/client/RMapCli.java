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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.ratis.rmap.common.FileQuorumSupplier;
import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.common.RMapName;

/**
 * Command line interface for RMaps
 */
public class RMapCli {

  private int printUsage() {
    System.err.println("Usage: ratis rmap <command> [<args>]");
    System.err.println("Commands:");
    System.err.println("  create <rmap_name> <key_class> <value_class>");
    System.err.println("  put <rmap_name> <key> <value>");
    System.err.println("  get <rmap_name> <key>");
    System.err.println("  scan <rmap_name> <start_key> <end_key>");
    // TODO: scan
    return -1;
  }

  private int create(String[] args) throws ClassNotFoundException, IOException {
    if (args.length < 3) {
      return printUsage();
    }

    RMapInfo info = RMapInfo.newBuilder()
        .withName(RMapName.valueOf(args[0]))
        .withKeyClass(Class.forName(args[1]))
        .withValueClass(Class.forName(args[2]))
        .build();

    try (Client client = ClientFactory.getClient(new FileQuorumSupplier());
         Admin admin = client.getAdmin()) {
      info = admin.createRMap(info);
      System.out.println("Created rmap:" + info);
    }

    return 0;
  }

  private int put(String[] args) throws IOException {
    if (args.length < 3) {
      return printUsage();
    }

    long rmapId = Long.parseLong(args[0]);
    try (Client client = ClientFactory.getClient(new FileQuorumSupplier());
         RMap<String, String> rmap = client.getRMap(rmapId)) {
      rmap.put(args[1], args[2]);
      System.out.println("Put success rmap:" + rmapId + ", " + args[1] + "=" + args[2]);
    }

    return 0;
  }

  private int get(String[] args) throws IOException {
    if (args.length < 2) {
      return printUsage();
    }

    long rmapId = Long.parseLong(args[0]);
    try (Client client = ClientFactory.getClient(new FileQuorumSupplier());
         RMap<String, String> rmap = client.getRMap(rmapId)) {
      String val = rmap.get(args[1]);
      System.out.println(val);
    }

    return 0;
  }

  private int scan(String[] args) throws IOException {
    if (args.length < 1) {
      return printUsage();
    }

    long rmapId = Long.parseLong(args[0]);
    try (Client client = ClientFactory.getClient(new FileQuorumSupplier());
         RMap<String, String> rmap = client.getRMap(rmapId)) {
      Scan<String> scan = new Scan<>();
      if (args.length > 1) {
        String startKey = args[1];
        scan.setStartKey(startKey); // TODO: this should be setting the raw bytes instead
      }
      if (args.length > 2) {
        String endKey = args[2];
        scan.setEndKey(endKey);
      }

      for (Map.Entry<String,String> entry : rmap.scan(scan)) {
        System.out.println(entry.getKey() + "=" + entry.getValue());
      }
    }

    return 0;
  }

  public int run(String[] args) throws Exception {
    if (args.length <= 1) {
      return printUsage();
    }

    switch (args[0]) {
      case "create":
        return create(Arrays.copyOfRange(args, 1, args.length));
      case "put":
        return put(Arrays.copyOfRange(args, 1, args.length));
      case "get":
        return get(Arrays.copyOfRange(args, 1, args.length));
      case "scan":
        return scan(Arrays.copyOfRange(args, 1, args.length));
      default:
        return printUsage();
    }
  }

  public static void main(String[] args) throws Exception {
    int ret = new RMapCli().run(args);
    System.exit(ret);
  }
}
