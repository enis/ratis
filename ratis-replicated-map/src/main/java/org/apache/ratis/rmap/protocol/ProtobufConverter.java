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

package org.apache.ratis.rmap.protocol;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.ratis.rmap.client.Scan;
import org.apache.ratis.rmap.common.RMapInfo;
import org.apache.ratis.rmap.common.RMapName;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.shaded.proto.rmap.RMapProtos;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Action;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.ActionResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.CreateRMapRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.CreateRMapResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.Entry;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.GetRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.GetResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.ListRMapInfosRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.ListRMapInfosResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.MultiActionRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.MultiActionResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.PutRequest;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.PutResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.ScanResponse;
import org.apache.ratis.shaded.proto.rmap.RMapProtos.WALEntry;

public class ProtobufConverter {
  public static RMapProtos.RMapInfo toProto(RMapInfo rMapInfo) {
    RMapProtos.RMapInfo.Builder builder = RMapProtos.RMapInfo.newBuilder()
        .setRmapId(rMapInfo.getId())
        .setName(rMapInfo.getName().toString())
        .setKeyClass(rMapInfo.getKeyClass().getName())
        .setValueClass(rMapInfo.getValueClass().getName())
        .setKeySerdeClass(rMapInfo.getKeySerdeClass().getName())
        .setValueSerdeClass(rMapInfo.getValueSerdeClass().getName());

    if (rMapInfo.getKeyComparatorClass() != null) {
      builder.setKeyComparatorClass(rMapInfo.getKeyComparatorClass().getName());
    }

    return builder.build();
  }

  public static RMapInfo fromProto(RMapProtos.RMapInfo rMapInfo) throws IOException {
    try {
      RMapInfo.Builder builder = RMapInfo.newBuilder()
          .withId(rMapInfo.getRmapId())
          .withName(RMapName.valueOf(rMapInfo.getName()))
          .withKeyClass(Class.forName(rMapInfo.getKeyClass()))
          .withValueClass(Class.forName(rMapInfo.getValueClass()))
          .withKeySerdeClass(Class.forName(rMapInfo.getKeySerdeClass()))
          .withValueSerdeClass(Class.forName(rMapInfo.getValueSerdeClass()));

      if (rMapInfo.getKeyComparatorClass() != null && !rMapInfo.getKeyComparatorClass().isEmpty()) {
        builder.withKeyComparatorClass(Class.forName(rMapInfo.getKeyComparatorClass()));
      }
      return builder.build();
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
  }

  public static Request buildCreateRMapRequest(RMapInfo rMapInfo) {
    CreateRMapRequest req = CreateRMapRequest.newBuilder()
        .setRmapInfo(toProto(rMapInfo))
        .build();

    return new Request(
        RMapProtos.Request.newBuilder()
        .setCreateRmapRequest(req)
        .build());
  }

  public static Response buildCreateRMapResponse(RMapProtos.RMapInfo rMapInfo) {
    CreateRMapResponse resp = CreateRMapResponse.newBuilder()
        .setRmapInfo(rMapInfo)
        .build();

    return new Response(
        RMapProtos.Response.newBuilder()
            .setCreateRmapResponse(resp)
            .build());

  }

  public static Request buildListRMapInfosRequest(long rMapId, String namePattern) {
    ListRMapInfosRequest.Builder req = ListRMapInfosRequest.newBuilder();

    if (rMapId > 0) {
      req.setRmapId(rMapId);
    }
    if (namePattern != null) {
      req.setNamePattern(namePattern);
    }

    return new Request(
        RMapProtos.Request.newBuilder()
            .setListRmapInfosRequest(req)
            .build());
  }

  public static Response buildListRMapInfosResponse(List<RMapProtos.RMapInfo> rMapInfos) {
    ListRMapInfosResponse.Builder resp = ListRMapInfosResponse.newBuilder()
        .addAllRmapInfo(rMapInfos);

    return new Response(
        RMapProtos.Response.newBuilder()
            .setListRmapInfosResponse(resp)
            .build());
  }

  public static Request buildMultiPutRequest(long rMapId, ByteString key, ByteString value) {
    MultiActionRequest.Builder req = MultiActionRequest.newBuilder();

    req.addAction(Action.newBuilder()
        .setPutRequest(PutRequest.newBuilder() // TODO: allow more than 1 K,V
            .setKey(key)
            .setValue(value)
    )).setRmapId(rMapId);

    return new Request(
        RMapProtos.Request.newBuilder()
            .setMultiActionRequest(req)
            .build());
  }

  public static Response buildMultiPutResponse(Stream<WALEntry> walEntries) {
    MultiActionResponse.Builder resp = MultiActionResponse.newBuilder();

    walEntries.forEach( walEntry ->
      resp.addActionResponse(ActionResponse.newBuilder()
          .setPutResponse(PutResponse.newBuilder())));

    return new Response(
        RMapProtos.Response.newBuilder()
            .setMultiActionResponse(resp)
            .build());
  }

  public static Request buildMultiGetRequest(long rMapId, ByteString key) {
    MultiActionRequest.Builder req = MultiActionRequest.newBuilder();

    req.addAction(Action.newBuilder()
        .setGetRequest(GetRequest.newBuilder() // TODO: allow more than 1 K,V
            .setKey(key)
        )).setRmapId(rMapId);

    return new Request(
        RMapProtos.Request.newBuilder()
            .setMultiActionRequest(req)
            .build());
  }

  public static Response buildMultiGetResponse(Stream<Entry> entries) {
    MultiActionResponse.Builder resp = MultiActionResponse.newBuilder();

    entries.forEach(entry ->
      resp.addActionResponse(ActionResponse.newBuilder()
          .setGetResponse(GetResponse.newBuilder()
              .setFound(entry.getValue() != null && !entry.getValue().isEmpty()) // TODO
              .setKey(entry.getKey())
              .setValue(entry.getValue()))));

    return new Response(
        RMapProtos.Response.newBuilder()
            .setMultiActionResponse(resp)
            .build());
  }

  public static <K, V> Response buildScanResponse(Iterator<Map.Entry> it,
                                                  Serde<K> keySerde, Serde<V> valueSerde,
                                                  boolean keysOnly) {
    ScanResponse.Builder resp = ScanResponse.newBuilder();

    it.forEachRemaining(entry -> {
      Entry.Builder entryBuilder = Entry.newBuilder()
          .setKey(keySerde.serialize((K)entry.getKey()));
      if (!keysOnly) {
        entryBuilder.setValue(valueSerde.serialize((V)entry.getValue()));
      }
      resp.addEntry(entryBuilder);
    });

    return new Response(
        RMapProtos.Response.newBuilder()
            .setScanResponse(resp)
            .build());
  }

  public static RMapProtos.Request parseRequestFrom(ByteString buf)
      throws InvalidProtocolBufferException {
    return RMapProtos.Request.parseFrom(buf);
  }

  public static RMapProtos.Response parseResponseFrom(ByteString buf)
      throws InvalidProtocolBufferException {
    return RMapProtos.Response.parseFrom(buf);
  }

  public static <K> RMapProtos.Scan toProto(Scan<K> scan, Serde<K> keySerde) {
    RMapProtos.Scan.Builder builder = RMapProtos.Scan.newBuilder()
        .setStartKeyInclusive(scan.isStartKeyInclusive())
        .setEndKeyInclusive(scan.isEndKeyInclusive())
        .setKeysOnly(scan.isKeysOnly())
        .setLimit(scan.getLimit());

    if (scan.getStartKey() != null) {
      builder.setStartKey(keySerde.serialize(scan.getStartKey()));
    }

    if (scan.getEndKey() != null) {
      builder.setEndKey(keySerde.serialize(scan.getEndKey()));
    }

    return builder.build();
  }

  public static <K> Request buildScanRequest(long rMapId, Scan<K> scan,
                                                            Serde<K> keySerde) {

    RMapProtos.ScanRequest req = RMapProtos.ScanRequest.newBuilder()
        .setRmapId(rMapId)
        .setScan(toProto(scan, keySerde))
        .build();

    return new Request(
        RMapProtos.Request.newBuilder()
            .setScanRequest(req)
            .build());
  }
}
