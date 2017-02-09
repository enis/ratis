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

import org.apache.ratis.rmap.common.RMapId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRMapId {

  static final Logger LOG = LoggerFactory.getLogger(TestRMapId.class);

  @Test
  public void testCreateUnique() {
    RMapId id = RMapId.createUnique();
    LOG.info("Created rmap_id:" + id);
  }

  @Test
  public void testValueOf() {
    RMapId id = RMapId.createUnique();
    assertEquals(id, RMapId.valueOf(id.toString()));

    assertEquals("a", RMapId.valueOf("a").toString());
    assertEquals("foo", RMapId.valueOf("foo").toString());
    assertEquals("foo_bar", RMapId.valueOf("foo_bar").toString());
    assertEquals("foo-bar", RMapId.valueOf("foo-bar").toString());
    assertEquals("foo-123", RMapId.valueOf("foo-123").toString());
    assertEquals("123", RMapId.valueOf("123").toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFormat() {
    RMapId.valueOf("rmap foo-bar-baz");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFormat2() {
    RMapId.valueOf("_");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFormat3() {
    RMapId.valueOf("-");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFormat4() {
    RMapId.valueOf(" ");
  }
}
