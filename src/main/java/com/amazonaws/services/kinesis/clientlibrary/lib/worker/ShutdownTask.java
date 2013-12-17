/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

/**
 * Task for invoking the RecordProcessor shutdown() callback.
 */
class ShutdownTask implements ITask {

    private static final Log LOG = LogFactory.getLog(ShutdownTask.class);
    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ShutdownReason reason;
    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final InitialPositionInStream initialPositionInStream;
    private final boolean cleanupLeasesOfCompletedShards;
    private final TaskType taskType = TaskType.SHUTDOWN;
    private final long backoffTimeMillis;

    /**
     * Constructor.
     */
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    ShutdownTask(ShardInfo shardInfo,
            IRecordProcessor recordProcessor,
            RecordProcessorCheckpointer recordProcessorCheckpointer,
            ShutdownReason reason,
            IKinesisProxy kinesisProxy,
            InitialPositionInStream initialPositionInStream,
            boolean cleanupLeasesOfCompletedShards,
            ILeaseManager<KinesisClientLease> leaseManager,
            long backoffTimeMillis) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.reason = reason;
        this.kinesisProxy = kinesisProxy;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
        this.leaseManager = leaseManager;
        this.backoffTimeMillis = backoffTimeMillis;
    }

    /* Invokes RecordProcessor shutdown() API.
     * (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        Exception exception = null;
        boolean applicationException = false;

        try {
            // If we reached end of the shard, set sequence number to SHARD_END.
            if (reason == ShutdownReason.TERMINATE) {
                recordProcessorCheckpointer.setSequenceNumber(SentinelCheckpoint.SHARD_END.toString());
            }
            
            LOG.debug("Invoking shutdown() for shard " + shardInfo.getShardId() + ", concurrencyToken "
                    + shardInfo.getConcurrencyToken() + ". Shutdown reason: " + reason);
            try {
                recordProcessor.shutdown(recordProcessorCheckpointer, reason);
                String lastCheckpointValue = recordProcessorCheckpointer.getLastCheckpointValue();
                if (reason == ShutdownReason.TERMINATE) {
                    if ((lastCheckpointValue == null)
                            || (!lastCheckpointValue.equals(SentinelCheckpoint.SHARD_END.toString()))) {
                        throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                                + shardInfo.getShardId());
                    }
                }
                LOG.debug("Record processor completed shutdown() for shard " + shardInfo.getShardId());
            } catch (Exception e) {
                applicationException = true;
                throw e;
            }
            
            if (reason == ShutdownReason.TERMINATE) {
                LOG.debug("Looking for child shards of shard " + shardInfo.getShardId());
                // create leases for the child shards
                ShardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy,
                        leaseManager,
                        initialPositionInStream,
                        cleanupLeasesOfCompletedShards);
                LOG.debug("Finished checking for child shards of shard " + shardInfo.getShardId());
            }

            return new TaskResult(null);
        } catch (Exception e) {
            if (applicationException) {
                LOG.error("Application exception. ", e);
            } else {
                LOG.error("Caught exception: ", e);
            }
            exception = e;
            // backoff if we encounter an exception.
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                LOG.debug("Interrupted sleep", ie);
            }
        }

        return new TaskResult(exception);
    }

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#getTaskType()
     */
    @Override
    public TaskType getTaskType() {
        return taskType;
    }

}
