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

import java.io.Closeable;
import java.io.IOException;

import org.apache.ratis.rmap.common.RMapName;

/**
 * Client maintains the connection to the quorum. Instances of RMaps can be created from
 * the client to read and write data.
 */
public interface Client extends Closeable {
  /**
   * Returns an Admin instance to do DDL operations
   * @return
   */
  Admin getAdmin();

  /**
   * Creates and returns an RMap instance to access the map data.
   * @param id
   * @param <K>
   * @param <V>
   * @return
   */
  <K,V> RMap<K,V> getRMap(RMapName id);

  @Override
  void close() throws IOException;
}
