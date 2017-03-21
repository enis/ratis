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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.rmap.client.Admin;
import org.apache.ratis.rmap.client.Client;
import org.apache.ratis.rmap.client.ClientFactory;
import org.apache.ratis.rmap.client.RMap;
import org.apache.ratis.rmap.client.Scan;
import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.common.RMapName;
import org.apache.ratis.rmap.protocol.Serde.StringSerde;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRMapEndToEnd {
  static final Logger LOG = LoggerFactory.getLogger(TestRMapEndToEnd.class);

  private static final int NUM_SERVERS = 3;

  private MiniRMapCluster cluster;

  @Before
  public void setUp() throws IOException {
    RaftProperties props = new RaftProperties();
    cluster = MiniRMapCluster.newBuilder()
        .withNumServers(NUM_SERVERS)
        .withRaftProperties(props)
        .build();
    cluster.start();
  }

  @After
  public void tearDown() {
    cluster.shutdown();
  }

  private Client createClient() {
    return ClientFactory.getClient(() -> cluster.getMiniCluster().getPeers());
  }

  private RMapInfo createRMapInfo() {
    RMapName name = RMapName.createUnique();
    return RMapInfo.newBuilder()
        .withName(name)
        .withKeyClass(String.class)
        .withValueClass(String.class)
        .withKeySerdeClass(StringSerde.class)
        .withValueSerdeClass(StringSerde.class)
        .build();
  }

  @Test
  public void testCreateRMap() throws IOException {
    Client client = createClient();
    RMapInfo info = createRMapInfo();

    try(Admin admin = client.getAdmin()) {
      RMapInfo newInfo = admin.createRMap(info);
      assertEquals(info.getName(), newInfo.getName());
    }
  }

  @Test
  public void testListRMaps() throws IOException {
    Client client = createClient();
    RMapInfo info = createRMapInfo();

    try(Admin admin = client.getAdmin()) {
      RMapInfo newInfo = admin.createRMap(info);
      List<RMapInfo> infos = admin.listRMapInfos();
      assertEquals(2, infos.size());
      assertEquals(infos.get(1).getName(), newInfo.getName());
    }
  }

  @Test
  public void testGetRMapInfo() throws IOException {
    Client client = createClient();
    RMapInfo info = createRMapInfo();

    try(Admin admin = client.getAdmin()) {
      RMapInfo newInfo = admin.createRMap(info);
      newInfo = admin.getRmapInfo(newInfo.getId());
      assertNotNull(newInfo);
      assertEquals(info.getName(), newInfo.getName());
    }
  }

  @Test
  public void testPutGet() throws Exception {
    Client client = createClient();
    RMapInfo info = createRMapInfo();

    try(Admin admin = client.getAdmin()) {
      RMapInfo newInfo = admin.createRMap(info);
      assertEquals(info.getName(), newInfo.getName());

      try(RMap<String, String> map = client.getRMap(newInfo.getId())) {
        map.put("foo", "bar");
        LOG.info("Get: foo={}", map.get("foo"));
        assertEquals("bar", map.get("foo"));
      }
    }
  }

  @Test
  public void testScan() throws Exception {
    Client client = createClient();
    RMapInfo info = createRMapInfo();

    try(Admin admin = client.getAdmin()) {
      RMapInfo newInfo = admin.createRMap(info);
      assertEquals(info.getName(), newInfo.getName());



      try(RMap<String, String> map = client.getRMap(newInfo.getId())) {
        TreeMap<String, String> sourceMap = new TreeMap<>();
        sourceMap.put("faa", "bar1");
        sourceMap.put("fee", "bar2");
        sourceMap.put("fii", "bar3");
        sourceMap.put("foo", "bar4");

        // put the entries from sourceMap to the rmap
        for (Map.Entry<String, String> entry : sourceMap.entrySet()) {
          map.put(entry.getKey(), entry.getValue());
        }

        System.out.println(" -- Results with default scan");
        TreeMap<String, String> expectedMap = new TreeMap<>();
        expectedMap.putAll(sourceMap);
        TreeMap<String, String> resultsMap = new TreeMap<>();
        for (Map.Entry<String, String> entry : map.scan(new Scan<>())) {
          System.out.println(entry.getKey() + "=" + entry.getValue());
          resultsMap.put(entry.getKey(), entry.getValue());
        }
        assertEquals(expectedMap, resultsMap);

        System.out.println(" -- Results with startKey=fee");
        resultsMap.clear();
        expectedMap.clear();
        expectedMap.putAll(sourceMap.tailMap("fee"));
        for (Map.Entry<String, String> entry : map.scan(new Scan<String>().setStartKey("fee"))) {
          System.out.println(entry.getKey() + "=" + entry.getValue());
          resultsMap.put(entry.getKey(), entry.getValue());
        }
        assertEquals(expectedMap, resultsMap);

        System.out.println(" -- Results with startKey=fee, startKeyInclusive=false");
        resultsMap.clear();
        expectedMap.clear();
        expectedMap.putAll(sourceMap.tailMap("fee", false));
        for (Map.Entry<String, String> entry : map.scan(
            new Scan<String>().setStartKey("fee").setStartKeyInclusive(false))) {
          System.out.println(entry.getKey() + "=" + entry.getValue());
          resultsMap.put(entry.getKey(), entry.getValue());
        }
        assertEquals(expectedMap, resultsMap);

        System.out.println(" -- Results with startKey=fee, startKeyInclusive=false, endKey=foo");
        resultsMap.clear();
        expectedMap.clear();
        expectedMap.putAll(sourceMap.subMap("fee", false, "foo", false));
        for (Map.Entry<String, String> entry : map.scan(
            new Scan<String>().setStartKey("fee").setStartKeyInclusive(false)
                .setEndKey("foo"))) {
          System.out.println(entry.getKey() + "=" + entry.getValue());
          resultsMap.put(entry.getKey(), entry.getValue());
        }
        assertEquals(expectedMap, resultsMap);

        System.out.println(" -- Results with startKey=fee, startKeyInclusive=false, endKey=foo, " +
            "endKeyInclusive=true");
        resultsMap.clear();
        expectedMap.clear();
        expectedMap.putAll(sourceMap.subMap("fee", false, "foo", true));
        for (Map.Entry<String, String> entry : map.scan(
            new Scan<String>().setStartKey("fee").setStartKeyInclusive(false)
                .setEndKey("foo").setEndKeyInclusive(true))) {
          System.out.println(entry.getKey() + "=" + entry.getValue());
          resultsMap.put(entry.getKey(), entry.getValue());
        }
        assertEquals(expectedMap, resultsMap);

        System.out.println(" -- Results with limit");
        resultsMap.clear();
        expectedMap.clear();
        expectedMap.putAll(sourceMap.subMap("faa", "foo"));
        for (Map.Entry<String, String> entry : map.scan(new Scan<String>().setLimit(3))) {
          System.out.println(entry.getKey() + "=" + entry.getValue());
          resultsMap.put(entry.getKey(), entry.getValue());
        }
        assertEquals(expectedMap, resultsMap);

        System.out.println(" -- Results with keysOnly");
        resultsMap.clear();
        expectedMap.clear();
        sourceMap.forEach((k,v) -> expectedMap.put(k, ""));
        for (Map.Entry<String, String> entry : map.scan(new Scan<String>().setKeysOnly(true))) {
          System.out.println(entry.getKey() + "=" + entry.getValue());
          resultsMap.put(entry.getKey(), entry.getValue());
        }
        assertEquals(expectedMap, resultsMap);
      }
    }
  }
}
