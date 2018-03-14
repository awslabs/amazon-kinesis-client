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
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer;
import software.amazon.kinesis.processor.v2.IRecordProcessor;
import software.amazon.kinesis.retrieval.GetRecordsCache;
import software.amazon.kinesis.retrieval.IKinesisProxy;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.leases.KinesisClientLease;
import software.amazon.kinesis.leases.ILeaseManager;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.MetricsLevel;
import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

/**
 * Task for invoking the RecordProcessor shutdown() callback.
 */
@Slf4j
public class ShutdownTask implements ITask {
    private static final String RECORD_PROCESSOR_SHUTDOWN_METRIC = "RecordProcessor.shutdown";

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ShutdownReason reason;
    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final InitialPositionInStreamExtended initialPositionInStream;
    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;
    private final TaskType taskType = TaskType.SHUTDOWN;
    private final long backoffTimeMillis;
    private final GetRecordsCache getRecordsCache;

    /**
     * Constructor.
     */
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    ShutdownTask(ShardInfo shardInfo,
                 IRecordProcessor recordProcessor,
                 RecordProcessorCheckpointer recordProcessorCheckpointer,
                 ShutdownReason reason,
                 IKinesisProxy kinesisProxy,
                 InitialPositionInStreamExtended initialPositionInStream,
                 boolean cleanupLeasesOfCompletedShards,
                 boolean ignoreUnexpectedChildShards,
                 ILeaseManager<KinesisClientLease> leaseManager,
                 long backoffTimeMillis, 
                 GetRecordsCache getRecordsCache) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.reason = reason;
        this.kinesisProxy = kinesisProxy;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.leaseManager = leaseManager;
        this.backoffTimeMillis = backoffTimeMillis;
        this.getRecordsCache = getRecordsCache;
    }

    /*
     * Invokes RecordProcessor shutdown() API.
     * (non-Javadoc)
     * 
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        Exception exception;
        boolean applicationException = false;

        try {
            // If we reached end of the shard, set sequence number to SHARD_END.
            if (reason == ShutdownReason.TERMINATE) {
                recordProcessorCheckpointer.setSequenceNumberAtShardEnd(
                        recordProcessorCheckpointer.getLargestPermittedCheckpointValue());
                recordProcessorCheckpointer.setLargestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
            }

            log.debug("Invoking shutdown() for shard {}, concurrencyToken {}. Shutdown reason: {}",
                    shardInfo.getShardId(), shardInfo.getConcurrencyToken(), reason);
            final ShutdownInput shutdownInput = new ShutdownInput()
                    .withShutdownReason(reason)
                    .withCheckpointer(recordProcessorCheckpointer);
            final long recordProcessorStartTimeMillis = System.currentTimeMillis();
            try {
                recordProcessor.shutdown(shutdownInput);
                ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.getLastCheckpointValue();

                if (reason == ShutdownReason.TERMINATE) {
                    if ((lastCheckpointValue == null)
                            || (!lastCheckpointValue.equals(ExtendedSequenceNumber.SHARD_END))) {
                        throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                                + shardInfo.getShardId());
                    }
                }
                log.debug("Shutting down retrieval strategy.");
                getRecordsCache.shutdown();
                log.debug("Record processor completed shutdown() for shard {}", shardInfo.getShardId());
            } catch (Exception e) {
                applicationException = true;
                throw e;
            } finally {
                MetricsHelper.addLatency(RECORD_PROCESSOR_SHUTDOWN_METRIC, recordProcessorStartTimeMillis,
                        MetricsLevel.SUMMARY);
            }

            if (reason == ShutdownReason.TERMINATE) {
                log.debug("Looking for child shards of shard {}", shardInfo.getShardId());
                // create leases for the child shards
                ShardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy,
                        leaseManager,
                        initialPositionInStream,
                        cleanupLeasesOfCompletedShards,
                        ignoreUnexpectedChildShards);
                log.debug("Finished checking for child shards of shard {}", shardInfo.getShardId());
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
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#getTaskType()
     */
    @Override
    public TaskType getTaskType() {
        return taskType;
    }

    @VisibleForTesting
    public ShutdownReason getReason() {
        return reason;
    }

}
