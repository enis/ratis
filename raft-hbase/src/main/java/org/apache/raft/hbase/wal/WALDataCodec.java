package org.apache.raft.hbase.wal;


import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

import com.google.protobuf.ByteString;

public interface WALDataCodec {

  interface Encoder extends Closeable {
    ByteString write(WALKey walKey, WALEdit walEdit) throws IOException;
  }

  interface Decoder extends Closeable {
    void read(ByteString bytes, WALKey walKey, WALEdit walEdit) throws IOException;
  }

  Encoder getEncoder();

  Decoder getDecoder();
}
