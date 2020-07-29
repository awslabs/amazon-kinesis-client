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

import com.amazonaws.services.kinesis.leases.LeasePendingDeletion;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.BlockedOnParentShardException;
import com.amazonaws.services.kinesis.leases.exceptions.CustomerApplicationException;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.LeaseCleanupManager;
import com.amazonaws.services.kinesis.leases.impl.UpdateField;
import com.amazonaws.services.kinesis.model.ChildShard;
import com.amazonaws.util.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Task for invoking the RecordProcessor shutdown() callback.
 */
class ShutdownTask implements ITask {

    private static final Log LOG = LogFactory.getLog(ShutdownTask.class);

    @VisibleForTesting
    static final int RETRY_RANDOM_MAX_RANGE = 50;

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
    private final LeaseCleanupManager leaseCleanupManager;

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
            ShardSyncStrategy shardSyncStrategy, List<ChildShard> childShards,
            LeaseCleanupManager leaseCleanupManager) {
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
        this.leaseCleanupManager = leaseCleanupManager;
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

        LOG.info("Invoking shutdown() for shard " + shardInfo.getShardId() + ", concurrencyToken: "
                         + shardInfo.getConcurrencyToken() + ", original Shutdown reason: " + reason + ". childShards:" + childShards);

        try {
            final KinesisClientLease currentShardLease = leaseCoordinator.getCurrentlyHeldLease(shardInfo.getShardId());
            final Runnable leaseLostAction = () -> takeLeaseLostAction();

            if (reason == ShutdownReason.TERMINATE) {
                try {
                    takeShardEndAction(currentShardLease);
                } catch (InvalidStateException e) {
                    // If InvalidStateException happens, it indicates we have a non recoverable error in short term.
                    // In this scenario, we should shutdown the shardConsumer with ZOMBIE reason to allow other worker to take the lease and retry shutting down.
                    LOG.warn("Lease " + shardInfo.getShardId() + ": Invalid state encountered while shutting down shardConsumer with TERMINATE reason. " +
                                     "Dropping the lease and shutting down shardConsumer using ZOMBIE reason. ", e);
                    dropLease(currentShardLease);
                    throwOnApplicationException(leaseLostAction);
                }
            } else {
                throwOnApplicationException(leaseLostAction);
            }

            LOG.debug("Shutting down retrieval strategy.");
            getRecordsCache.shutdown();
            LOG.debug("Record processor completed shutdown() for shard " + shardInfo.getShardId());
            return new TaskResult(null);
        } catch (Exception e) {
            if (e instanceof CustomerApplicationException) {
                LOG.error("Shard " + shardInfo.getShardId() + ": Application exception: ", e);
            } else {
                LOG.error("Shard " + shardInfo.getShardId() + ": Caught exception: ", e);
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

    // Involves persisting child shard info, attempt to checkpoint and enqueueing lease for cleanup.
    private void takeShardEndAction(KinesisClientLease currentShardLease)
        throws InvalidStateException, DependencyException, ProvisionedThroughputException, CustomerApplicationException {
        // Create new lease for the child shards if they don't exist.
        // We have one valid scenario that shutdown task got created with SHARD_END reason and an empty list of childShards.
        // This would happen when KinesisDataFetcher catches ResourceNotFound exception.
        // In this case, KinesisDataFetcher will send out SHARD_END signal to trigger a shutdown task with empty list of childShards.
        // This scenario could happen when customer deletes the stream while leaving the KCL application running.
        if (currentShardLease == null) {
            throw new InvalidStateException("Shard " + shardInfo.getShardId() + ": Lease not owned by the current worker. Leaving ShardEnd handling to new owner.");
        }
        if (!CollectionUtils.isNullOrEmpty(childShards)) {
            // If childShards is not empty, create new leases for the childShards and update the current lease with the childShards lease information.
            createLeasesForChildShardsIfNotExist();
            updateCurrentLeaseWithChildShards(currentShardLease);
        } else {
            LOG.warn("Shard " + shardInfo.getShardId()
                             + ": Shutting down consumer with SHARD_END reason without creating leases for child shards.");
        }
        // Checkpoint with SHARD_END sequence number.
        final LeasePendingDeletion leasePendingDeletion = new LeasePendingDeletion(currentShardLease, shardInfo);
        if (!leaseCleanupManager.isEnqueuedForDeletion(leasePendingDeletion)) {
            boolean isSuccess = false;
            try {
                isSuccess = attemptShardEndCheckpointing();
            } finally {
                // Check if either the shard end ddb persist is successful or
                // if childshards is empty. When child shards is empty then either it is due to
                // completed shard being reprocessed or we got RNF from service.
                // For these cases enqueue the lease for deletion.
                if (isSuccess || CollectionUtils.isNullOrEmpty(childShards)) {
                    leaseCleanupManager.enqueueForDeletion(leasePendingDeletion);
                }
            }
        }
    }

    private void takeLeaseLostAction() {
        final ShutdownInput leaseLostShutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.ZOMBIE)
                .withCheckpointer(recordProcessorCheckpointer);
        recordProcessor.shutdown(leaseLostShutdownInput);
    }

    private boolean attemptShardEndCheckpointing()
        throws DependencyException, ProvisionedThroughputException, InvalidStateException, CustomerApplicationException {
        final KinesisClientLease leaseFromDdb = Optional.ofNullable(leaseCoordinator.getLeaseManager().getLease(shardInfo.getShardId()))
                .orElseThrow(() -> new InvalidStateException("Lease for shard " + shardInfo.getShardId() + " does not exist."));
        if (!leaseFromDdb.getCheckpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
            // Call the recordProcessor to checkpoint with SHARD_END sequence number.
            // The recordProcessor.shutdown is implemented by customer. We should validate if the SHARD_END checkpointing is successful after calling recordProcessor.shutdown.
            throwOnApplicationException(() -> applicationCheckpointAndVerification());
        }
        return true;
    }

    private void applicationCheckpointAndVerification() {
        recordProcessorCheckpointer.setSequenceNumberAtShardEnd(
                recordProcessorCheckpointer.getLargestPermittedCheckpointValue());
        recordProcessorCheckpointer.setLargestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
        final ShutdownInput shardEndShutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.TERMINATE)
                .withCheckpointer(recordProcessorCheckpointer);
        recordProcessor.shutdown(shardEndShutdownInput);

        final ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.getLastCheckpointValue();

        final boolean successfullyCheckpointedShardEnd = lastCheckpointValue.equals(ExtendedSequenceNumber.SHARD_END);

        if ((lastCheckpointValue == null) || (!successfullyCheckpointedShardEnd)) {
            throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                                                       + shardInfo.getShardId() + ". Application must checkpoint upon shutdown. " +
                                                       "See IRecordProcessor.shutdown javadocs for more information.");
        }
    }

    private void throwOnApplicationException(Runnable action) throws CustomerApplicationException {
        try {
            action.run();
        } catch (Exception e) {
            throw new CustomerApplicationException("Customer application throws exception for shard " + shardInfo.getShardId(), e);
        }
    }

    private void createLeasesForChildShardsIfNotExist() throws InvalidStateException, DependencyException, ProvisionedThroughputException {
        // For child shard resulted from merge of two parent shards, verify if both the parents are either present or
        // not present in the lease table before creating the lease entry.
        if (!CollectionUtils.isNullOrEmpty(childShards) && childShards.size() == 1) {
            final ChildShard childShard = childShards.get(0);
            final List<String> parentLeaseKeys = childShard.getParentShards();

            if (parentLeaseKeys.size() != 2) {
                throw new InvalidStateException("Shard " + shardInfo.getShardId()+ "'s only child shard " + childShard
                                                        + " does not contain other parent information.");
            } else {
                boolean isValidLeaseTableState = Objects.isNull(leaseCoordinator.getLeaseManager().getLease(parentLeaseKeys.get(0))) ==
                                                 Objects.isNull(leaseCoordinator.getLeaseManager().getLease(parentLeaseKeys.get(1)));
                if (!isValidLeaseTableState) {
                    if(!isOneInNProbability(RETRY_RANDOM_MAX_RANGE)) {
                        throw new BlockedOnParentShardException(
                                "Shard " + shardInfo.getShardId() + "'s only child shard " + childShard
                                        + " has partial parent information in lease table. Hence deferring lease creation of child shard.");
                    } else {
                        throw new InvalidStateException("Shard " + shardInfo.getShardId() + "'s only child shard " + childShard
                                                                + " has partial parent information in lease table.");
                    }
                }
            }
        }
        // Attempt create leases for child shards.
        for (ChildShard childShard : childShards) {
            final String leaseKey = childShard.getShardId();
            if (leaseCoordinator.getLeaseManager().getLease(leaseKey) == null) {
                final KinesisClientLease leaseToCreate = KinesisShardSyncer.newKCLLeaseForChildShard(childShard);
                leaseCoordinator.getLeaseManager().createLeaseIfNotExists(leaseToCreate);
                LOG.info("Shard " + shardInfo.getShardId() + " : Created child shard lease: " + leaseToCreate.getLeaseKey());
            }
        }
    }

    /**
     * Returns true for 1 in N probability.
     */
    @VisibleForTesting
    boolean isOneInNProbability(int n) {
        Random r = new Random();
        return 1 == r.nextInt((n - 1) + 1) + 1;
    }

    private void updateCurrentLeaseWithChildShards(KinesisClientLease currentLease) throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final Set<String> childShardIds = childShards.stream().map(ChildShard::getShardId).collect(Collectors.toSet());
        currentLease.setChildShardIds(childShardIds);
        leaseCoordinator.getLeaseManager().updateLeaseWithMetaInfo(currentLease, UpdateField.CHILD_SHARDS);
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

    private void dropLease(KinesisClientLease currentShardLease) {
        if (currentShardLease == null) {
            LOG.warn("Shard " + shardInfo.getShardId() + ": Unable to find the lease for shard. Will shutdown the shardConsumer directly.");
            return;
        }
        leaseCoordinator.dropLease(currentShardLease);
        LOG.warn("Dropped lease for shutting down ShardConsumer: " + currentShardLease.getLeaseKey());
    }
}
