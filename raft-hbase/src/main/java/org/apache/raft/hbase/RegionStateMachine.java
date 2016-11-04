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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.WriteContext;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.wal.WALSplitter.MutationReplay;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.hbase.wal.PBWALDataCodec;
import org.apache.raft.hbase.wal.RaftWAL;
import org.apache.raft.hbase.wal.WALDataCodec;
import org.apache.raft.proto.RaftProtos.SMLogEntryProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.statemachine.BaseStateMachine;
import org.apache.raft.statemachine.TrxContext;

import com.google.common.collect.Lists;

public class RegionStateMachine extends BaseStateMachine {

  private final Configuration hbaseConf;
  private final FileSystem fs;
  private HRegion region = null;
  protected WALDataCodec codec;
  protected WALDataCodec.Encoder encoder;
  protected WALDataCodec.Decoder decoder;

  public RegionStateMachine() throws IOException {
    this.hbaseConf = HBaseConfiguration.create();
    this.fs = FileSystem.getLocal(this.hbaseConf);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public void initialize(RaftProperties properties, RaftStorage storage) throws IOException {
    HTableDescriptor htd = HBaseUtils.createTableDescriptor();
    HRegionInfo hri = HBaseUtils.createRegionInfo(htd);
    Path rootDir = new Path(storage.getStorageDir().getRoot().toString()); // TODO

    this.region = HRegion.createHRegion(hri, rootDir, hbaseConf, htd,
      new RaftWAL(rootDir, hbaseConf, Lists.newArrayList()));
    this.codec = PBWALDataCodec.create(hbaseConf);
    this.encoder = codec.getEncoder();
    this.decoder = codec.getDecoder();
  }

  private static class BatchContext<T> {
    private WriteContext<T> writeContext;
    private RegionActionResult.Builder regionActionResultBuilder;

    BatchContext() {
      this.regionActionResultBuilder = RegionActionResult.newBuilder();
    }

    public void setWriteContext(WriteContext<T> writeContext) {
      this.writeContext = writeContext;
    }

    public WriteContext<T> getWriteContext() {
      return writeContext;
    }
  }

  @Override
  public TrxContext startTransaction(RaftClientRequest request) throws IOException {
    Message msg = request.getMessage();
    MultiRequest multi = MultiRequest.parseFrom(msg.getContent());

    List<CellScannable> cellsToReturn = null;
    MultiResponse.Builder responseBuilder = MultiResponse.newBuilder();

    BatchContext batchContext = new BatchContext();

    for (RegionAction regionAction : multi.getRegionActionList()) {
      // TODO: get the region here
      startNonAtomicRegionMutation(batchContext, region, regionAction);
    }

    WriteContext writeContext = batchContext.getWriteContext();
    SMLogEntryProto.Builder builder = SMLogEntryProto.newBuilder();
    builder.setData(encoder.write(writeContext.getWalKey(), writeContext.getWalEdit()));
    return new TrxContext(this, request, builder.build(), batchContext);
  }

  @Override
  public TrxContext preAppendTransaction(TrxContext trx) {
    BatchContext batchContext = (BatchContext) trx.getStateMachineContext().get();
    // stamp sequence id
    region.preAppend(batchContext.getWriteContext());
    return super.preAppendTransaction(trx);
  }

  @Override
  public TrxContext applyTransactionSerial(TrxContext trx) throws IOException {
    Optional<Object> stateMachineContext = trx.getStateMachineContext();
    if (stateMachineContext.isPresent()) {
      // this the leader applying already committed entries
      BatchContext batchContext = (BatchContext) stateMachineContext.get();
      WriteContext writeContext = batchContext.getWriteContext();
      region.applyCommittedSerial(writeContext);
    } else {
      // this is a follower applying committed entries from the leader
      WALKey walKey = new WALKey();
      WALEdit walEdit = new WALEdit();
      decoder.read(trx.getSMLogEntry().get().getData(), walKey, walEdit);

      // start replay
      WriteContext writeContext = region.startReplay(walKey, walEdit);
      region.applyCommittedSerial(writeContext);

      // save the state for later
      BatchContext batchContext = new BatchContext();
      batchContext.writeContext = writeContext;
      trx.setStateMachineContext(batchContext);
    }

    return trx;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TrxContext trx) throws IOException {
    Optional<Object> stateMachineContext = trx.getStateMachineContext();
    assert stateMachineContext.isPresent();

    BatchContext batchContext = (BatchContext) stateMachineContext.get();
    WriteContext writeContext = batchContext.getWriteContext();

    region.applyCommitted(writeContext);
    return null;
  }

  public HRegion getRegion() {
    return region;
  }

  private void startNonAtomicRegionMutation(final BatchContext batchContext,
                                                           final HRegion region,
                                                           final RegionAction actions) {
    // Gather up CONTIGUOUS Puts and Deletes in this mutations List.  Idea is that rather than do
    // one at a time, we instead pass them in batch.  Be aware that the corresponding
    // ResultOrException instance that matches each Put or Delete is then added down in the
    // doBatchOp call.  We should be staying aligned though the Put and Delete are deferred/batched
    List<ClientProtos.Action> mutations = null;
    IOException sizeIOE = null;
    Object lastBlock = null;
    for (ClientProtos.Action action : actions.getActionList()) {
      ClientProtos.ResultOrException.Builder resultOrExceptionBuilder = null;
      try {
        Result r = null;

        if (action.hasGet()) {
          long before = EnvironmentEdgeManager.currentTime();
          try {
            // TODO: gets in multi should be handled differently.
            Get get = ProtobufUtil.toGet(action.getGet());
            r = region.get(get);
          } finally {
          }
        } else if (action.hasMutation()) {
          MutationType type = action.getMutation().getMutateType();
          if (type != MutationType.PUT && type != MutationType.DELETE && mutations != null &&
              !mutations.isEmpty()) {
            // Flush out any Puts or Deletes already collected.
            startBatchOp(batchContext, region, mutations);
            mutations.clear();
          }
          switch (type) {
            case APPEND:
              //r = append(region, quota, action.getMutation(), cellScanner, nonceGroup);
              break;
            case INCREMENT:
              //r = increment(region, quota, action.getMutation(), cellScanner, nonceGroup);
              break;
            case PUT:
            case DELETE:
              // Collect the individual mutations and apply in a batch
              if (mutations == null) {
                mutations = new ArrayList<ClientProtos.Action>(actions.getActionCount());
              }
              mutations.add(action);
              break;
            default:
              throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
          }
        } else {
          throw new HBaseIOException("Unexpected Action type");
        }
//        if (r != null) {
//          ClientProtos.Result pbResult = null;
//          if (isClientCellBlockSupport(context)) {
//            pbResult = ProtobufUtil.toResultNoData(r);
//            //  Hard to guess the size here.  Just make a rough guess.
//            if (cellsToReturn == null) {
//              cellsToReturn = new ArrayList<CellScannable>();
//            }
//            cellsToReturn.add(r);
//          } else {
//            pbResult = ProtobufUtil.toResult(r);
//          }
//          lastBlock = addSize(context, r, lastBlock);
//          resultOrExceptionBuilder =
//            ClientProtos.ResultOrException.newBuilder().setResult(pbResult);
//        }
        // Could get to here and there was no result and no exception.  Presumes we added
        // a Put or Delete to the collecting Mutations List for adding later.  In this
        // case the corresponding ResultOrException instance for the Put or Delete will be added
        // down in the doBatchOp method call rather than up here.
      } catch (IOException ie) {
        resultOrExceptionBuilder = ResultOrException.newBuilder().
          setException(ResponseConverter.buildException(ie));
      }
      if (resultOrExceptionBuilder != null) {
        // Propagate index.
        resultOrExceptionBuilder.setIndex(action.getIndex());
        batchContext.regionActionResultBuilder.addResultOrException(resultOrExceptionBuilder.build());
      }
    }
    // Finish up any outstanding mutations
    if (mutations != null && !mutations.isEmpty()) {
      startBatchOp(batchContext, region, mutations);
    }
  }

  /**
   * Execute a list of Put/Delete mutations.
   *
   * @param region
   * @param mutations
   */
  private void startBatchOp(BatchContext batchContext,
                            final HRegion region, final List<ClientProtos.Action> mutations) {
    Mutation[] mArray = new Mutation[mutations.size()];
    long before = EnvironmentEdgeManager.currentTime();
    boolean batchContainsPuts = false, batchContainsDelete = false;
    try {
      int i = 0;
      for (ClientProtos.Action action: mutations) {
        MutationProto m = action.getMutation();
        Mutation mutation;
        if (m.getMutateType() == MutationType.PUT) {
          mutation = ProtobufUtil.toPut(m, null);
          batchContainsPuts = true;
        } else {
          mutation = ProtobufUtil.toDelete(m, null);
          batchContainsDelete = true;
        }
        mArray[i++] = mutation;
      }

      WriteContext<?> ctx = region.startBatchMutate(mArray, HConstants.NO_NONCE,
          HConstants.NO_NONCE);
      batchContext.setWriteContext(ctx);

//      for (i = 0; i < codes.length; i++) {
//        int index = mutations.get(i).getIndex();
//        Exception e = null;
//        switch (codes[i].getOperationStatusCode()) {
//          case BAD_FAMILY:
//            e = new NoSuchColumnFamilyException(codes[i].getExceptionMsg());
//            builder.addResultOrException(getResultOrException(e, index));
//            break;
//
//          case SANITY_CHECK_FAILURE:
//            e = new FailedSanityCheckException(codes[i].getExceptionMsg());
//            builder.addResultOrException(getResultOrException(e, index));
//            break;
//
//          default:
//            e = new DoNotRetryIOException(codes[i].getExceptionMsg());
//            builder.addResultOrException(getResultOrException(e, index));
//            break;
//
//          case SUCCESS:
//            builder.addResultOrException(getResultOrException(
//              ClientProtos.Result.getDefaultInstance(), index));
//            break;
//        }
//      }
    } catch (IOException ie) {
      for (int i = 0; i < mutations.size(); i++) {
        batchContext.regionActionResultBuilder.addResultOrException(getResultOrException(ie, mutations.get(i).getIndex()));
      }
    }
  }

  private OperationStatus[] doReplayBatchOp(final HRegion region,
                                            final List<WALSplitter.MutationReplay> mutations,
                                            long replaySeqId) throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    boolean batchContainsPuts = false, batchContainsDelete = false;
    try {
      for (Iterator<MutationReplay> it = mutations.iterator(); it.hasNext();) {
        WALSplitter.MutationReplay m = it.next();

        if (m.type == MutationType.PUT) {
          batchContainsPuts = true;
        } else {
          batchContainsDelete = true;
        }

        NavigableMap<byte[], List<Cell>> map = m.mutation.getFamilyCellMap();
        List<Cell> metaCells = map.get(WALEdit.METAFAMILY);
        if (metaCells != null && !metaCells.isEmpty()) {
          it.remove(); // just skip meta markers
        }
      }
      if (!region.getRegionInfo().isMetaTable()) {
        // TODO regionServer.cacheFlusher.reclaimMemStoreMemory();
      }
      return region.batchReplay(mutations.toArray(
          new WALSplitter.MutationReplay[mutations.size()]), replaySeqId);
    } finally {
    }
  }

  private Result get(Get get, HRegion region, RpcCallContext context) throws IOException {
    // region.prepareGet(get);
    List<Cell> results = new ArrayList<Cell>();
    boolean stale = region.getRegionInfo().getReplicaId() != 0;
    // pre-get CP hook
    if (region.getCoprocessorHost() != null) {
      if (region.getCoprocessorHost().preGet(get, results)) {
        return Result
            .create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
      }
    }
    long before = EnvironmentEdgeManager.currentTime();
    Scan scan = new Scan(get);

    RegionScanner scanner = null;
    try {
      scanner = region.getScanner(scan);
      scanner.next(results);
    } finally {
      if (scanner != null) {
//        if (closeCallBack == null) {
//          // If there is a context then the scanner can be added to the current
//          // RpcCallContext. The rpc callback will take care of closing the
//          // scanner, for eg in case
//          // of get()
//          assert scanner instanceof org.apache.hadoop.hbase.ipc.RpcCallback;
//          context.setCallBack((RegionScannerImpl) scanner);
//        } else {
//          // The call is from multi() where the results from the get() are
//          // aggregated and then send out to the
//          // rpc. The rpccall back will close all such scanners created as part
//          // of multi().
//          closeCallBack.addScanner(scanner);
//        }
      }
    }

    // post-get CP hook
    if (region.getCoprocessorHost() != null) {
      region.getCoprocessorHost().postGet(get, results);
    }
    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
  }

  private static ResultOrException getResultOrException(final ClientProtos.Result r,
      final int index){
    return getResultOrException(ResponseConverter.buildActionResult(r), index);
  }

  private static ResultOrException getResultOrException(final Exception e, final int index) {
    return getResultOrException(ResponseConverter.buildActionResult(e), index);
  }

  private static ResultOrException getResultOrException(
      final ResultOrException.Builder builder, final int index) {
    return builder.setIndex(index).build();
  }

  @Override
  public void setRaftConfiguration(RaftConfiguration conf) {
    // TODO Auto-generated method stub
  }

  @Override
  public RaftConfiguration getRaftConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }
}
