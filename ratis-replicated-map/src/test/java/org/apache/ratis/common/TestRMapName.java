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

package org.apache.ratis.common;

import static org.junit.Assert.assertEquals;

import org.apache.ratis.rmap.common.RMapName;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRMapName {

  static final Logger LOG = LoggerFactory.getLogger(TestRMapName.class);

  @Test
  public void testCreateUnique() {
    RMapName id = RMapName.createUnique();
    LOG.info("Created rmap_id:" + id);
  }

  @Test
  public void testValueOf() {
    RMapName id = RMapName.createUnique();
    assertEquals(id, RMapName.valueOf(id.toString()));

    assertEquals("a", RMapName.valueOf("a").toString());
    assertEquals("foo", RMapName.valueOf("foo").toString());
    assertEquals("foo_bar", RMapName.valueOf("foo_bar").toString());
    assertEquals("foo-bar", RMapName.valueOf("foo-bar").toString());
    assertEquals("foo-123", RMapName.valueOf("foo-123").toString());
    assertEquals("123", RMapName.valueOf("123").toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFormat() {
    RMapName.valueOf("rmap foo-bar-baz");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFormat2() {
    RMapName.valueOf("_");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFormat3() {
    RMapName.valueOf("-");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFormat4() {
    RMapName.valueOf(" ");
  }
}
