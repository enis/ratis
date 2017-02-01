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

import java.util.UUID;

/**
 * RMapId is a handle for the RMap instance that uniquely identifies the replicated map
 * in the cluster. The format of the id is: rmap_UUID
 */
public class RMapId implements Comparable<RMapId> {
  private static final String PREFIX = "rmap_";
  private static final int UUID_LENGTH = 16;

  private final String id;

  private RMapId(String id) {
    this.id = id;
  }

  public static RMapId valueOf(String rmapId) throws IllegalArgumentException {
    checkFormat(rmapId);
    return new RMapId(rmapId);
  }

  /**
   * Creates a new unique id. Uses pseudo-random UUID generator.
   * @return a unique rmapId
   */
  public static RMapId create() {
    UUID uuid = UUID.randomUUID();
    String str = uuid.toString();
    return new RMapId(PREFIX + str);
  }

  public String toString() {
    return id;
  }

  private static void checkFormat(String id) {
    // no need to pattern match.
    if (!id.startsWith(PREFIX)) {
      throw new IllegalArgumentException("rmapId passed is not of the form: rmap_UUID, " +
          "passed:" +  id);
    }
    // parse the string to make sure that it conforms to the UUID format
    UUID.fromString(id.substring(PREFIX.length()));
  }

  @Override
  public int compareTo(RMapId o) {
    return this.id.compareTo(o.id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RMapId) {
      return this.compareTo((RMapId) obj) == 0;
    }
    return false;
  }
}
