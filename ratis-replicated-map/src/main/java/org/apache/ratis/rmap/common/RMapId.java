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

import com.google.common.base.Preconditions;

/**
 * RMapId is a handle for the RMap instance that uniquely identifies the replicated map
 * in the cluster.
 */
public class RMapId implements Comparable<RMapId> {
  private static final String PREFIX = "rmap_";
  private final String id;

  private RMapId(String id) {
    this.id = id;
  }

  public static RMapId valueOf(String id) throws IllegalArgumentException {
    checkFormat(id);
    return new RMapId(id);
  }

  /**
   * Creates a new unique id. Uses pseudo-random UUID generator.
   * @return a unique rmapId
   */
  public static RMapId createUnique() {
    UUID uuid = UUID.randomUUID();
    String str = uuid.toString();
    return new RMapId(PREFIX + str);
  }

  /**
   * Check whether the format is [a-zA-Z0-9][a-zA-Z_0-9-]+.
   * @param id the id
   */
  private static void checkFormat(String id) {
    Preconditions.checkArgument(id.length() > 0);
    Preconditions.checkArgument(Character.isLetterOrDigit(id.charAt(0)));
    for (int i = 1; i < id.length(); i++) {
      Character c = id.charAt(i);
      if (Character.isLetterOrDigit(c) ||
          c == '_' ||
          c == '-') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character code:" + c +
          " at " + i + ", ids can only contain alphanumeric characters': i.e. [a-zA-Z_0-9-]. " +
          " given id: " + id);
    }

  }

  public String toString() {
    return id;
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
