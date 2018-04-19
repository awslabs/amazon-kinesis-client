/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License. 
 */
package software.amazon.kinesis.lifecycle;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.google.common.annotations.VisibleForTesting;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.checkpoint.RecordProcessorCheckpointer;
import software.amazon.kinesis.leases.LeaseManager;
import software.amazon.kinesis.leases.KinesisClientLease;
import software.amazon.kinesis.leases.LeaseManagerProxy;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardSyncer;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.retrieval.GetRecordsCache;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Task for invoking the RecordProcessor shutdown() callback.
 */
@RequiredArgsConstructor
@Slf4j
public class ShutdownTask implements ITask {
    private static final String RECORD_PROCESSOR_SHUTDOWN_METRIC = "RecordProcessor.shutdown";

    @NonNull
    private final ShardInfo shardInfo;
    @NonNull
    private final LeaseManagerProxy leaseManagerProxy;
    @NonNull
    private final RecordProcessor recordProcessor;
    @NonNull
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    @NonNull
    private final ShutdownReason reason;
    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;
    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;
    @NonNull
    private final LeaseManager<KinesisClientLease> leaseManager;
    private final long backoffTimeMillis;
    @NonNull
    private final GetRecordsCache getRecordsCache;

    private final TaskType taskType = TaskType.SHUTDOWN;
    private TaskCompletedListener listener;

    /*
     * Invokes RecordProcessor shutdown() API.
     * (non-Javadoc)
     * 
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        try {
            Exception exception;
            boolean applicationException = false;

            try {
                // If we reached end of the shard, set sequence number to SHARD_END.
                if (reason == ShutdownReason.TERMINATE) {
                    recordProcessorCheckpointer.sequenceNumberAtShardEnd(
                            recordProcessorCheckpointer.largestPermittedCheckpointValue());
                    recordProcessorCheckpointer.largestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
                }

                log.debug("Invoking shutdown() for shard {}, concurrencyToken {}. Shutdown reason: {}",
                        shardInfo.shardId(), shardInfo.concurrencyToken(), reason);
                final ShutdownInput shutdownInput = new ShutdownInput()
                        .shutdownReason(reason)
                        .checkpointer(recordProcessorCheckpointer);
                final long recordProcessorStartTimeMillis = System.currentTimeMillis();
                try {
                    recordProcessor.shutdown(shutdownInput);
                    ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.lastCheckpointValue();

                    if (reason == ShutdownReason.TERMINATE) {
                        if ((lastCheckpointValue == null)
                                || (!lastCheckpointValue.equals(ExtendedSequenceNumber.SHARD_END))) {
                            throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                                    + shardInfo.shardId());
                        }
                    }
                    log.debug("Shutting down retrieval strategy.");
                    getRecordsCache.shutdown();
                    log.debug("Record processor completed shutdown() for shard {}", shardInfo.shardId());
                } catch (Exception e) {
                    applicationException = true;
                    throw e;
                } finally {
                    MetricsHelper.addLatency(RECORD_PROCESSOR_SHUTDOWN_METRIC, recordProcessorStartTimeMillis,
                            MetricsLevel.SUMMARY);
                }

                if (reason == ShutdownReason.TERMINATE) {
                    log.debug("Looking for child shards of shard {}", shardInfo.shardId());
                    // create leases for the child shards
                    ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy,
                            leaseManager,
                            initialPositionInStream,
                            cleanupLeasesOfCompletedShards,
                            ignoreUnexpectedChildShards);
                    log.debug("Finished checking for child shards of shard {}", shardInfo.shardId());
                }

                return new TaskResult(null);
            } catch (Exception e) {
                if (applicationException) {
                    log.error("Application exception. ", e);
                } else {
                    log.error("Caught exception: ", e);
                }
                exception = e;
                // backoff if we encounter an exception.
                try {
                    Thread.sleep(this.backoffTimeMillis);
                } catch (InterruptedException ie) {
                    log.debug("Interrupted sleep", ie);
                }
            }

            return new TaskResult(exception);
        } finally {
            if (listener != null) {
                listener.taskCompleted(this);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#taskType()
     */
    @Override
    public TaskType taskType() {
        return taskType;
    }

    @Override
    public void addTaskCompletedListener(TaskCompletedListener taskCompletedListener) {
        if (listener != null) {
            log.warn("Listener is being reset, this shouldn't happen");
        }
        listener = taskCompletedListener;
    }

    @VisibleForTesting
    public ShutdownReason getReason() {
        return reason;
    }

}
