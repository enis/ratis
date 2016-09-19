/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.raft;

import static org.apache.raft.RaftTestUtil.waitForLeader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.hbase.HBaseUtils;
import org.apache.raft.hbase.RegionStateMachine;
import org.apache.raft.hbase.client.HBaseClient;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.raft.server.simulation.SimulatedRequestReply;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TestRaftRegionStateMachine {

  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private final MiniRaftClusterWithSimulatedRpc cluster;

  public TestRaftRegionStateMachine() throws IOException {
    final RaftProperties properties = getProperties();
    if (ThreadLocalRandom.current().nextBoolean()) {
      // turn off simulate latency half of the times.
      properties.setInt(SimulatedRequestReply.SIMULATE_LATENCY_KEY, 0);
    }
    cluster = new MiniRaftClusterWithSimulatedRpc(NUM_SERVERS, properties);
  }

  public MiniRaftClusterWithSimulatedRpc getCluster() {
    return cluster;
  }

  static final Logger LOG = LoggerFactory.getLogger(RaftBasicTests.class);

  public static final int NUM_SERVERS = 5;

  private final RaftProperties properties = new RaftProperties();

  {
    properties.setBoolean(RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY, false);
    // TODO: use guice or something?
    properties.setStrings(RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_KEY,
      RegionStateMachine.class.getName());
  }

  public RaftProperties getProperties() {
    return properties;
  }

  @Before
  public void setup() throws IOException {
    Assert.assertNull(getCluster().getLeader());
    getCluster().start();
  }

  @After
  public void tearDown() {
    final MiniRaftCluster cluster = getCluster();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBasicAppendEntries() throws Exception {
    final MiniRaftCluster cluster = getCluster();
    RaftServer leader = waitForLeader(cluster);
    final String killed = cluster.getFollowers().get(3).getId();
    cluster.killServer(killed);
    LOG.info(cluster.printServers());

    final RaftClient client = cluster.createClient("client", null);
    final HBaseClient hbaseClient = new HBaseClient(client);

    for (int i = 0; i < 100; i++) {
      byte[] b = Bytes.toBytes(i);
      hbaseClient.put(Lists.newArrayList(
        new Put(b).addImmutable(HBaseUtils.FAMILY, b, b)));
    }

    Thread.sleep(cluster.getMaxTimeout() + 100);
    LOG.info(cluster.printAllLogs());

    cluster.getServers().stream().filter(RaftServer::isRunning)
      .forEach(s -> {
        RegionStateMachine rsm = (RegionStateMachine) s.getState().getStateMachine();
        HRegion region = rsm.getRegion();
        System.out.println("---------------");
        System.out.format("id=%s\n", s.getId());
        scanRegion(region, cell -> System.out.println(cell.toString()));
        });

//    cluster.getServers().stream().filter(RaftServer::isRunning)
//        .map(s -> s.getState().getLog().getEntries(1, Long.MAX_VALUE))
//        .forEach(e -> RaftTestUtil.assertLogEntries(e, 1, term, messages));
  }

  private void scanRegion(HRegion region, Consumer<Cell> f) {
    List<Cell> cells = Lists.newArrayList();
    try (RegionScanner scanner = region.getScanner(new Scan())) {
      while(true) {
        cells.clear();
        boolean more = scanner.next(cells);
        cells.stream().forEach(f);
        if (!more) break;
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
