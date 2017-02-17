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

import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.Message;

/**
 * Serializer and Deserializer. Operates to / from T from / to ByteString
 */
public interface Serde<T> {

  ByteString serialize(T value);

  T deserialize(ByteString buf);

  class StringSerde implements Serde<String> {
    @Override
    public ByteString serialize(String value) {
      // TODO: make this zero-copy
      return ByteString.copyFromUtf8(value);
    }

    public String deserialize(ByteString buf) {
      return buf.toStringUtf8();
    }
  }

  class ProtobufSerde<T extends Message> implements Serde<T> {
    @Override
    public ByteString serialize(T value) {
      return value.toByteString();
    }

    @Override
    public T deserialize(ByteString buf) {
      // TODO
      return null;
    }
  }

  class ByteStringSerde implements Serde<ByteString> {
    @Override
    public ByteString serialize(ByteString value) {
      return value;
    }

    @Override
    public ByteString deserialize(ByteString buf) {
      return buf;
    }
  }
}
