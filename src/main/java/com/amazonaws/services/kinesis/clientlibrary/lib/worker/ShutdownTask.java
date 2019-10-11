/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.util.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Task for invoking the RecordProcessor shutdown() callback.
 */
class ShutdownTask implements ITask {

    private static final Log LOG = LogFactory.getLog(ShutdownTask.class);

    private static final String RECORD_PROCESSOR_SHUTDOWN_METRIC = "RecordProcessor.shutdown";

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ShutdownReason reason;
    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private final InitialPositionInStreamExtended initialPositionInStream;
    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;
    private final TaskType taskType = TaskType.SHUTDOWN;
    private final long backoffTimeMillis;
    private final GetRecordsCache getRecordsCache;
    private final ShardSyncer shardSyncer;
    private final ShardSyncStrategy shardSyncStrategy;

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
            KinesisClientLibLeaseCoordinator leaseCoordinator,
            long backoffTimeMillis,
            GetRecordsCache getRecordsCache, ShardSyncer shardSyncer, ShardSyncStrategy shardSyncStrategy) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.reason = reason;
        this.kinesisProxy = kinesisProxy;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.leaseManager = leaseCoordinator.getLeaseManager();
        this.leaseCoordinator = leaseCoordinator;
        this.backoffTimeMillis = backoffTimeMillis;
        this.getRecordsCache = getRecordsCache;
        this.shardSyncer = shardSyncer;
        this.shardSyncStrategy = shardSyncStrategy;
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
            ShutdownReason localReason = reason;
            List<Shard> allShards = new ArrayList<>();
            /*
             * Revalidate if the current shard is closed before shutting down the shard consumer with reason SHARD_END
             * If current shard is not closed, shut down the shard consumer with reason LEASE_LOST that allows other
             * shard consumer to subscribe to this shard.
             */
            if(localReason == ShutdownReason.TERMINATE) {
                allShards = kinesisProxy.getShardList();

                if(!CollectionUtils.isNullOrEmpty(allShards) && !validateShardEnd(allShards)) {
                    localReason = ShutdownReason.ZOMBIE;
                    forceLoseLease();
                    LOG.debug("Force the lease to be lost before shutting down the consumer.");
                }
            }


            // If we reached end of the shard, set sequence number to SHARD_END.
            if (localReason == ShutdownReason.TERMINATE) {
                recordProcessorCheckpointer.setSequenceNumberAtShardEnd(
                        recordProcessorCheckpointer.getLargestPermittedCheckpointValue());
                recordProcessorCheckpointer.setLargestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
            }

            LOG.debug("Invoking shutdown() for shard " + shardInfo.getShardId() + ", concurrencyToken "
                    + shardInfo.getConcurrencyToken() + ". Shutdown reason: " + localReason);
            final ShutdownInput shutdownInput = new ShutdownInput()
                    .withShutdownReason(localReason)
                    .withCheckpointer(recordProcessorCheckpointer);
            final long recordProcessorStartTimeMillis = System.currentTimeMillis();
            try {
                recordProcessor.shutdown(shutdownInput);
                ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.getLastCheckpointValue();

                if (localReason == ShutdownReason.TERMINATE) {
                    if ((lastCheckpointValue == null)
                            || (!lastCheckpointValue.equals(ExtendedSequenceNumber.SHARD_END))) {
                        throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                                + shardInfo.getShardId() + ". Application must checkpoint upon shutdown. " +
                                "See IRecordProcessor.shutdown javadocs for more information.");
                    }
                }
                LOG.debug("Shutting down retrieval strategy.");
                getRecordsCache.shutdown();
                LOG.debug("Record processor completed shutdown() for shard " + shardInfo.getShardId());
            } catch (Exception e) {
                applicationException = true;
                throw e;
            } finally {
                MetricsHelper.addLatency(RECORD_PROCESSOR_SHUTDOWN_METRIC, recordProcessorStartTimeMillis,
                        MetricsLevel.SUMMARY);
            }

            if (localReason == ShutdownReason.TERMINATE) {
                LOG.debug("Looking for child shards of shard " + shardInfo.getShardId());
                // create leases for the child shards
                TaskResult result = shardSyncStrategy.onShardConsumerShutDown(allShards);
                if (result.getException() != null) {
                    LOG.debug("Exception while trying to sync shards on the shutdown of shard: " + shardInfo
                            .getShardId());
                    throw result.getException();
                }
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
    ShutdownReason getReason() {
        return reason;
    }

    private boolean validateShardEnd(List<Shard> shards) {
        for(Shard shard : shards) {
            if (isChildShardOfCurrentShard(shard)) {
                return true;
            }
        }
        return false;
    }

    private boolean isChildShardOfCurrentShard(Shard shard) {
        return (shard.getParentShardId() != null && shard.getParentShardId().equals(shardInfo.getShardId())
                || shard.getAdjacentParentShardId() != null && shard.getAdjacentParentShardId().equals(shardInfo.getShardId()));
    }

    private void forceLoseLease() {
        Collection<KinesisClientLease> leases = leaseCoordinator.getAssignments();
        if(leases != null && !leases.isEmpty()) {
            for(KinesisClientLease lease : leases) {
                if(lease.getLeaseKey().equals(shardInfo.getShardId())) {
                    leaseCoordinator.dropLease(lease);
                }
            }
        }
    }
}
