package org.apache.raft.hbase.wal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.LimitInputStream;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

public class PBWALDataCodec extends Configured implements WALDataCodec {

  private static final Log LOG = LogFactory.getLog(PBWALDataCodec.class);

  private WALCellCodec codec;

  public static WALDataCodec create(Configuration conf) throws IOException {
    return new PBWALDataCodec(conf);
  }

  private PBWALDataCodec(Configuration conf) throws IOException {
    super(conf);
    this.codec = getCodec(conf, null);
  }

  @Override
  public WALDataCodec.Encoder getEncoder() {
    return new Encoder();
  }

  @Override
  public WALDataCodec.Decoder getDecoder() {
    return new Decoder();
  }

  private class Encoder implements WALDataCodec.Encoder {
    @Override
    public ByteString write(WALKey walKey, WALEdit walEdit) throws IOException {
      // TODO: This is extremely inefficient and not how we do this in HBase. Find a better way
      Output out = ByteString.newOutput();
      Codec.Encoder cellEncoder = codec.getEncoder(out);

      walKey.getBuilder(null).setFollowingKvCount(walEdit.size()).build()
          .writeDelimitedTo(out);
      for (Cell cell : walEdit.getCells()) {
        // cellEncoder must assume little about the stream, since we write PB and cells in turn.
        cellEncoder.write(cell);
      }
      return out.toByteString();
    }

    @Override
    public void close() throws IOException {
    }
  }

  private class Decoder implements WALDataCodec.Decoder {
    @Override
    public void read(ByteString bytes, WALKey walKey, WALEdit walEdit) throws IOException {
      InputStream inputStream = bytes.newInput();
      WALCellCodec.Decoder cellDecoder = codec.getDecoder(inputStream);
      // OriginalPosition might be < 0 on local fs; if so, it is useless to us.
      WALProtos.WALKey.Builder builder = WALProtos.WALKey.newBuilder();
      long size = 0;
      try {
        long available = -1;
        try {
          int firstByte = inputStream.read();
          if (firstByte == -1) {
            throw new EOFException("First byte is negative at offset 0");
          }
          size = CodedInputStream.readRawVarint32(firstByte, inputStream);
          // available may be < 0 on local fs for instance.  If so, can't depend on it.
          available = inputStream.available();
          if (available > 0 && available < size) {
            throw new EOFException("Available stream not enough for edit, " +
                "inputStream.available()= " + inputStream.available() + ", " +
                "entry size= " + size);
          }
          ProtobufUtil.mergeFrom(builder, new LimitInputStream(inputStream, size),
              (int) size);
        } catch (InvalidProtocolBufferException ipbe) {
          throw (EOFException) new EOFException("Invalid PB, EOF? Ignoring;" +
              ", messageSize=" + size + ", currentAvailable=" + available).initCause(ipbe);
        }
        if (!builder.isInitialized()) {
          // TODO: not clear if we should try to recover from corrupt PB that looks semi-legit.
          //       If we can get the KV count, we could, theoretically, try to get next record.
          throw new EOFException("Partial PB while reading WAL, " +
              "probably an unexpected EOF, ignoring.");
        }
        WALProtos.WALKey pbWalKey = builder.build();
        walKey.readFieldsFromPb(pbWalKey, null);
        if (!pbWalKey.hasFollowingKvCount() || 0 == pbWalKey.getFollowingKvCount()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("WALKey has no KVs that follow it; trying the next one. current");
          }
          throw new IOException("WALKey has no KVs that follow it");
        }
        int expectedCells = pbWalKey.getFollowingKvCount();
        try {
          int actualCells = walEdit.readFromCells(cellDecoder, expectedCells);
          if (expectedCells != actualCells) {
            throw new EOFException("Only read " + actualCells); // other info added in catch
          }
        } catch (Exception ex) {
          String message = " while reading " + expectedCells + " WAL KVs;";
          IOException realEofEx = extractHiddenEof(ex);
          throw (EOFException) new EOFException("EOF " + message).
              initCause(realEofEx != null ? realEofEx : ex);
        }
      } catch (EOFException eof) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Encountered a malformed edit");
        }
        throw eof;
      }
    }

    private IOException extractHiddenEof(Exception ex) {
      // There are two problems we are dealing with here. Hadoop stream throws generic exception
      // for EOF, not EOFException; and scanner further hides it inside RuntimeException.
      IOException ioEx = null;
      if (ex instanceof EOFException) {
        return (EOFException) ex;
      } else if (ex instanceof IOException) {
        ioEx = (IOException) ex;
      } else if (ex instanceof RuntimeException
          && ex.getCause() != null && ex.getCause() instanceof IOException) {
        ioEx = (IOException) ex.getCause();
      }
      if (ioEx != null) {
        if (ioEx.getMessage().contains("EOF")) return ioEx;
        return null;
      }
      return null;
    }

    @Override
    public void close() throws IOException {
    }
  }

  private WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
      throws IOException {
    return WALCellCodec.create(conf, null, compressionContext);
  }
}
