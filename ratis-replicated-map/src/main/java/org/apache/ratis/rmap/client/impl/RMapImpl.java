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

package org.apache.ratis.rmap.client.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.collections.keyvalue.DefaultMapEntry;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.rmap.client.Client;
import org.apache.ratis.rmap.client.RMap;
import org.apache.ratis.rmap.client.Scan;
import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.protocol.ProtobufConverter;
import org.apache.ratis.rmap.protocol.Request;
import org.apache.ratis.rmap.protocol.Serde;
import org.apache.ratis.rmap.util.ReflectionUtils;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.rmap.RMapProtos;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.GetResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.MultiActionResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Response;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.ScanResponse;

public class RMapImpl<K, V> implements RMap<K, V> {
  private final RaftClient raftClient;
  private final RMapInfo info;
  private final Client client;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;

  RMapImpl(RMapInfo info, ClientImpl client) {
    this.info = info;
    this.client = client;
    this.raftClient = client.getRaftClient();
    this.keySerde = ReflectionUtils.<Serde<K>>newInstance(info.getKeySerdeClass());
    this.valueSerde = ReflectionUtils.<Serde<V>>newInstance(info.getValueSerdeClass());
  }

  public void put(K key, V value) throws IOException {
    ByteString k = keySerde.serialize(key);
    ByteString v = valueSerde.serialize(value);

    Request req = ProtobufConverter.buildMultiPutRequest(info.getId(), k, v);
    RaftClientReply reply = raftClient.send(req);

    if (!reply.isSuccess()) {
      throw new IOException("Request failed :" + reply);
    }

    Response resp = ProtobufConverter.parseResponseFrom(reply.getMessage().getContent());
    MultiActionResponse multiActionResponse = resp.getMultiActionResponse();
    assert multiActionResponse != null;

    // nothing to check in the response
  }

  public V get(K key) throws IOException {
    ByteString k = keySerde.serialize(key);

    Request req = ProtobufConverter.buildMultiGetRequest(info.getId(), k);
    RaftClientReply reply = raftClient.sendReadOnly(req);

    if (!reply.isSuccess()) {
      throw new IOException("Request failed :" + reply);
    }

    Response resp = ProtobufConverter.parseResponseFrom(reply.getMessage().getContent());
    MultiActionResponse multiActionResponse = resp.getMultiActionResponse();
    assert multiActionResponse != null;

    GetResponse getResponse = multiActionResponse.getActionResponse(0).getGetResponse();
    if (!getResponse.getFound()) {
      return null;
    }

    return valueSerde.deserialize(getResponse.getValue());
  }

  public Iterable<Entry<K, V>> scan(Scan scan) throws IOException {
    // TODO: Scan should be using streaming grpc, however, for now it is single request/response.
    // Be careful for large scan results

    Request req = ProtobufConverter.buildScanRequest(info.getId(), scan, keySerde);
    RaftClientReply reply = raftClient.sendReadOnly(req);

    if (!reply.isSuccess()) {
      throw new IOException("Request failed :" + reply);
    }

    Response resp = ProtobufConverter.parseResponseFrom(reply.getMessage().getContent());
    ScanResponse scanResponse = resp.getScanResponse();
    assert scanResponse != null;

    return () -> new Iterator<Entry<K, V>>() {
      Iterator<RMapProtos.Entry> it = scanResponse.getEntryList().iterator();

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public Entry<K, V> next() {
        RMapProtos.Entry entry = it.next();
        return new DefaultMapEntry(
            keySerde.deserialize(entry.getKey()), valueSerde.deserialize(entry.getValue()));
      }
    };
  }

  // TODO: checkAndPut, putIfAbsent, etc
  // TODO: iterate

  @Override
  public void close() throws IOException {
  }
}
