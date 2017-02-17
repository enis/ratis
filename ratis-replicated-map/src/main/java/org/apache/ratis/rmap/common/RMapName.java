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
 * RMapName is a handle for the RMap instance that uniquely identifies the replicated map
 * in the cluster.
 */
public class RMapName implements Comparable<RMapName> {
  private static final String PREFIX = "rmap_";
  private final String name;

  private RMapName(String name) {
    this.name = name;
  }

  public static RMapName valueOf(String name) throws IllegalArgumentException {
    checkFormat(name);
    return new RMapName(name);
  }

  /**
   * Creates a new unique id. Uses pseudo-random UUID generator.
   * @return a unique rmap name
   */
  public static RMapName createUnique() {
    UUID uuid = UUID.randomUUID();
    String str = uuid.toString();
    return new RMapName(PREFIX + str);
  }

  /**
   * Check whether the format is [a-zA-Z0-9][a-zA-Z_0-9-]+.
   * @param name the name
   */
  private static void checkFormat(String name) {
    Preconditions.checkArgument(name.length() > 0);
    Preconditions.checkArgument(Character.isLetterOrDigit(name.charAt(0)));
    for (int i = 1; i < name.length(); i++) {
      Character c = name.charAt(i);
      if (Character.isLetterOrDigit(c) ||
          c == '_' ||
          c == '-') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character code:" + c +
          " at " + i + ", ids can only contain alphanumeric characters': i.e. [a-zA-Z_0-9-]. " +
          " given id: " + name);
    }
  }

  public String toString() {
    return name;
  }

  @Override
  public int compareTo(RMapName o) {
    return this.name.compareTo(o.name);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RMapName) {
      return this.compareTo((RMapName) obj) == 0;
    }
    return false;
  }
}
