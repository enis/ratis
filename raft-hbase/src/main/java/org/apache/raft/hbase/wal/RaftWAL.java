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

package org.apache.raft.hbase.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import com.google.common.collect.Sets;

public class RaftWAL implements WAL {
  private static final Log LOG = LogFactory.getLog(RaftWAL.class);

  class WALEntry extends Entry {
    // The below data members are denoted 'transient' just to highlight these are not persisted;
    // they are only in memory and held here while passing over the ring buffer.
    private final transient boolean inMemstore;
    private final transient HRegionInfo hri;
    private final transient Set<byte[]> familyNames;
    // In the new WAL logic, we will rewrite failed WAL entries to new WAL file, so we need to avoid
    // calling stampRegionSequenceId again.
    private transient boolean stamped = false;

    WALEntry(final WALKey key, final WALEdit edit,
        final HRegionInfo hri, final boolean inMemstore) {
      super(key, edit);
      this.inMemstore = inMemstore;
      this.hri = hri;
      if (inMemstore) {
        // construct familyNames here to reduce the work of log sinker.
        ArrayList<Cell> cells = this.getEdit().getCells();
        if (CollectionUtils.isEmpty(cells)) {
          this.familyNames = Collections.<byte[]> emptySet();
        } else {
          Set<byte[]> familySet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
          for (Cell cell : cells) {
            if (!CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
              // TODO: Avoid this clone?
              familySet.add(CellUtil.cloneFamily(cell));
            }
          }
          this.familyNames = Collections.unmodifiableSet(familySet);
        }
      } else {
        this.familyNames = Collections.<byte[]> emptySet();
      }
    }

    boolean isInMemstore() {
      return this.inMemstore;
    }

    HRegionInfo getHRegionInfo() {
      return this.hri;
    }

    /**
     * Here is where a WAL edit gets its sequenceid.
     * SIDE-EFFECT is our stamping the sequenceid into every Cell AND setting the sequenceid into the
     * MVCC WriteEntry!!!!
     * @return The sequenceid we stamped on this edit.
     */
    long stampRegionSequenceId() throws IOException {
      if (stamped) {
        return getKey().getSequenceId();
      }
      stamped = true;
      long regionSequenceId = WALKey.NO_SEQUENCE_ID;
      MultiVersionConcurrencyControl mvcc = getKey().getMvcc();
      MultiVersionConcurrencyControl.WriteEntry we = null;

      if (mvcc != null) {
        we = mvcc.begin();
        regionSequenceId = we.getWriteNumber();
      }

      if (!this.getEdit().isReplay() && inMemstore) {
        for (Cell c:getEdit().getCells()) {
          CellUtil.setSequenceId(c, regionSequenceId);
        }
      }
      getKey().setWriteEntry(we);
      return regionSequenceId;
    }

    /**
     * @return the family names which are effected by this edit.
     */
    Set<byte[]> getFamilyNames() {
      return familyNames;
    }
  }

  protected final List<WALActionsListener> listeners =
      new CopyOnWriteArrayList<WALActionsListener>();
  protected final Path path;
  protected final WALCoprocessorHost coprocessorHost;
  protected final AtomicBoolean closed = new AtomicBoolean(false);

  public RaftWAL(final Path path, final Configuration conf,
      final List<WALActionsListener> listeners) {
    this.coprocessorHost = new WALCoprocessorHost(this, conf);
    this.path = path;
    if (null != listeners) {
      for(WALActionsListener listener : listeners) {
        registerWALActionsListener(listener);
      }
    }
  }

  @Override
  public void registerWALActionsListener(final WALActionsListener listener) {
    listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(final WALActionsListener listener) {
    return listeners.remove(listener);
  }

  @Override
  public byte[][] rollWriter() {
    if (!listeners.isEmpty()) {
      for (WALActionsListener listener : listeners) {
        listener.logRollRequested(false);
      }
      for (WALActionsListener listener : listeners) {
        try {
          listener.preLogRoll(path, path);
        } catch (IOException exception) {
          LOG.debug("Ignoring exception from listener.", exception);
        }
      }
      for (WALActionsListener listener : listeners) {
        try {
          listener.postLogRoll(path, path);
        } catch (IOException exception) {
          LOG.debug("Ignoring exception from listener.", exception);
        }
      }
    }
    return null;
  }

  @Override
  public byte[][] rollWriter(boolean force) {
    return rollWriter();
  }

  @Override
  public void shutdown() {
    if(closed.compareAndSet(false, true)) {
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener listener : this.listeners) {
          listener.logCloseRequested();
        }
      }
    }
  }

  @Override
  public void close() {
    shutdown();
  }

  @Override
  public long append(HRegionInfo regionInfo, WALKey key, WALEdit edits, boolean inMemstore)
      throws IOException {

    WALEntry entry = new WALEntry(key, edits, regionInfo, inMemstore);
    long regionSequenceId = entry.stampRegionSequenceId();

    // Coprocessor hook.
    if (!coprocessorHost.preWALWrite(entry.getHRegionInfo(), entry.getKey(), entry.getEdit())) {
      if (entry.getEdit().isReplay()) {
        // Set replication scope null so that this won't be replicated
        entry.getKey().serializeReplicationScope(false);
      }
    }
    if (!listeners.isEmpty()) {
      for (WALActionsListener i : listeners) {
        i.visitLogEntryBeforeWrite(entry.getKey(), entry.getEdit());
      }
    }

    coprocessorHost.postWALWrite(entry.getHRegionInfo(), entry.getKey(), entry.getEdit());

    if (!this.listeners.isEmpty()) {
      final long start = System.nanoTime();
      long len = 0;
      for (Cell cell : edits.getCells()) {
        len += CellUtil.estimatedSerializedSizeOf(cell);
      }
      final long elapsed = (System.nanoTime() - start) / 1000000L;
      for (WALActionsListener listener : this.listeners) {
        listener.postAppend(len, elapsed, key, edits);
      }
    }
    return -1;
  }

  @Override
  public void updateStore(byte[] encodedRegionName, byte[] familyName,
      Long sequenceid, boolean onlyIfGreater) { return; }

  @Override
  public void sync() {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener listener : this.listeners) {
        listener.postSync(0l, 0);
      }
    }
  }

  @Override
  public void sync(long txid) {
    sync();
  }

  @Override
  public Long startCacheFlush(final byte[] encodedRegionName, Set<byte[]> flushedFamilyNames) {
    if (closed.get()) return null;
    return HConstants.NO_SEQNUM;
  }

  @Override
  public void completeCacheFlush(final byte[] encodedRegionName) {
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    return HConstants.NO_SEQNUM;
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
    return HConstants.NO_SEQNUM;
  }

  @Override
  public String toString() {
    return "WAL disabled.";
  }

  @Override
  public void logRollerExited() {
  }
}
