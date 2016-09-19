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

package org.apache.raft.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {

  public static TableName TABLE = TableName.valueOf("foo");
  public static byte[] FAMILY = Bytes.toBytes("f1");

  public static HTableDescriptor createTableDescriptor() {
    return new HTableDescriptor(TABLE)
        .addFamily(new HColumnDescriptor(FAMILY));
  }

  public static HRegionInfo createRegionInfo(HTableDescriptor htd) {
    return new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false);
  }
}
