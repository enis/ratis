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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rmap.common.FileQuorumSupplier;
import org.apache.ratis.rmap.common.QuorumSupplier;
import org.apache.ratis.shaded.com.google.common.collect.Lists;
import org.apache.ratis.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestFileQuorumSupplier {

  private String content1 = "s1:100";
  private String content2 = "s1:100\ns2:100";
  private String content3 = "# some comment\n"
                  + "      # some comment\n"
                  + "\n" // empty line
                  + "     \n"
                  + "\t\n"
                  + "s1:100\n"
                  + "# s2:100\n"
                  + "   s3:100   \n"
                  + "s4:100   \n";

  private TestUtil testUtil = new TestUtil();

  private File writeFile(String content) throws IOException {
    File dir = testUtil.getDataTestDir("test-file-quorum-supplier");
    File f = new File(dir, UUID.randomUUID().toString());
    FileWriter writer = new FileWriter(f);
    writer.append(content);
    writer.close();
    return f;
  }

  @Test
  public void testGetQuorumOne() throws IOException {
    File f1 = writeFile(content1);
    QuorumSupplier q = new FileQuorumSupplier(f1.getAbsolutePath());
    List<String> actual
        = q.getQuorum().stream().map(RaftPeer::getAddress).collect(Collectors.toList());
    Assert.assertEquals(Lists.newArrayList("s1:100"), actual);
  }

  @Test
  public void testGetQuorumTwo() throws IOException {
    File f1 = writeFile(content2);
    QuorumSupplier q = new FileQuorumSupplier(f1.getAbsolutePath());
    List<String> actual
        = q.getQuorum().stream().map(RaftPeer::getAddress).collect(Collectors.toList());
    Assert.assertEquals(Lists.newArrayList("s1:100", "s2:100"), actual);
  }

  @Test
  public void testGetQuorumThree() throws IOException {
    File f1 = writeFile(content3);
    QuorumSupplier q = new FileQuorumSupplier(f1.getAbsolutePath());
    List<String> actual
        = q.getQuorum().stream().map(RaftPeer::getAddress).collect(Collectors.toList());
    Assert.assertEquals(Lists.newArrayList("s1:100", "s3:100", "s4:100"), actual);
  }
}