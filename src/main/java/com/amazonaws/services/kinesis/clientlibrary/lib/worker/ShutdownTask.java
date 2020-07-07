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

import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.model.ChildShard;
import com.amazonaws.util.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private final InitialPositionInStreamExtended initialPositionInStream;
    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;
    private final TaskType taskType = TaskType.SHUTDOWN;
    private final long backoffTimeMillis;
    private final GetRecordsCache getRecordsCache;
    private final ShardSyncer shardSyncer;
    private final ShardSyncStrategy shardSyncStrategy;
    private final List<ChildShard> childShards;

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
            GetRecordsCache getRecordsCache, ShardSyncer shardSyncer,
            ShardSyncStrategy shardSyncStrategy, List<ChildShard> childShards) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.reason = reason;
        this.kinesisProxy = kinesisProxy;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.leaseCoordinator = leaseCoordinator;
        this.backoffTimeMillis = backoffTimeMillis;
        this.getRecordsCache = getRecordsCache;
        this.shardSyncer = shardSyncer;
        this.shardSyncStrategy = shardSyncStrategy;
        this.childShards = childShards;
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
            LOG.info("Invoking shutdown() for shard " + shardInfo.getShardId() + ", concurrencyToken: "
                     + shardInfo.getConcurrencyToken() + ", original Shutdown reason: " + reason + ". childShards:" + childShards);
            ShutdownReason localReason = reason;
            /*
             * Revalidate if the current shard is closed before shutting down the shard consumer with reason SHARD_END
             * If current shard is not closed, shut down the shard consumer with reason LEASE_LOST that allows active
             * workers to contend for the lease of this shard.
             */
            if(localReason == ShutdownReason.TERMINATE) {
                // Create new lease for the child shards if they don't exist.
                // We have one valid scenario that shutdown task got created with SHARD_END reason and an empty list of childShards.
                // This would happen when KinesisDataFetcher catches ResourceNotFound exception.
                // In this case, KinesisDataFetcher will send out SHARD_END signal to trigger a shutdown task with empty list of childShards.
                // This scenario could happen when customer deletes the stream while leaving the KCL application running.
                try {
                    if (!CollectionUtils.isNullOrEmpty(childShards)) {
                        createLeasesForChildShardsIfNotExist();
                        updateCurrentLeaseWithChildShards();
                    } else {
                        LOG.warn("Shard " + shardInfo.getShardId()
                                         + ": Shutting down consumer with SHARD_END reason without creating leases for child shards.");
                    }
                } catch (InvalidStateException e) {
                    // If invalidStateException happens, it indicates we are missing childShard related information.
                    // In this scenario, we should shutdown the shardConsumer with ZOMBIE reason to allow other worker to take the lease and retry getting
                    // childShard information in the processTask.
                    localReason = ShutdownReason.ZOMBIE;
                    dropLease();
                    LOG.warn("Shard " + shardInfo.getShardId() + ": Exception happened while shutting down shardConsumer with TERMINATE reason. " +
                                     "Dropping the lease and shutting down shardConsumer using ZOMBIE reason. Exception: ", e);
                }
            }

            // If we reached end of the shard, set sequence number to SHARD_END.
            if (localReason == ShutdownReason.TERMINATE) {
                recordProcessorCheckpointer.setSequenceNumberAtShardEnd(
                        recordProcessorCheckpointer.getLargestPermittedCheckpointValue());
                recordProcessorCheckpointer.setLargestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
            }

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

    private void createLeasesForChildShardsIfNotExist() throws InvalidStateException, DependencyException, ProvisionedThroughputException {
        for (ChildShard childShard : childShards) {
            final String leaseKey = childShard.getShardId();
            if (leaseCoordinator.getLeaseManager().getLease(leaseKey) == null) {
                final KinesisClientLease leaseToCreate = KinesisShardSyncer.newKCLLeaseForChildShard(childShard);
                leaseCoordinator.getLeaseManager().createLeaseIfNotExists(leaseToCreate);
                LOG.info("Shard " + shardInfo.getShardId() + " : Created child shard lease: " + leaseToCreate.getLeaseKey());
            }
        }
    }

    private void updateCurrentLeaseWithChildShards() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final KinesisClientLease currentLease = leaseCoordinator.getCurrentlyHeldLease(shardInfo.getShardId());
        if (currentLease == null) {
            throw new InvalidStateException("Failed to retrieve current lease for shard " + shardInfo.getShardId());
        }
        final Set<String> childShardIds = childShards.stream().map(ChildShard::getShardId).collect(Collectors.toSet());

        currentLease.setChildShardIds(childShardIds);
        final boolean updateResult = leaseCoordinator.updateLease(currentLease, UUID.fromString(shardInfo.getConcurrencyToken()));
        if (!updateResult) {
            throw new InvalidStateException("Failed to update parent lease with child shard information for shard " + shardInfo.getShardId());
        }
        LOG.info("Shard " + shardInfo.getShardId() + ": Updated current lease with child shard information: " + currentLease.getLeaseKey());
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

    private void dropLease() {
        KinesisClientLease lease = leaseCoordinator.getCurrentlyHeldLease(shardInfo.getShardId());
        if (lease == null) {
            LOG.warn("Shard " + shardInfo.getShardId() + ": Lease already dropped. Will shutdown the shardConsumer directly.");
            return;
        }
        leaseCoordinator.dropLease(lease);
        LOG.warn("Dropped lease for shutting down ShardConsumer: " + lease.getLeaseKey());
    }
}
