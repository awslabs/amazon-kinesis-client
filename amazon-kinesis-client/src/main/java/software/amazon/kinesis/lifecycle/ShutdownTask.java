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
package software.amazon.kinesis.lifecycle;

import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.exceptions.internal.BlockedOnParentShardException;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.UpdateField;
import software.amazon.kinesis.leases.exceptions.CustomerApplicationException;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.LeasePendingDeletion;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Task for invoking the ShardRecordProcessor shutdown() callback.
 */
@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class ShutdownTask implements ConsumerTask {
    private static final String SHUTDOWN_TASK_OPERATION = "ShutdownTask";
    private static final String RECORD_PROCESSOR_SHUTDOWN_METRIC = "RecordProcessor.shutdown";

    /**
     * Reusable, immutable {@link LeaseLostInput}.
     */
    private static final LeaseLostInput LEASE_LOST_INPUT = LeaseLostInput.builder().build();

    private static final Random RANDOM = new Random();

    @VisibleForTesting
    static final int RETRY_RANDOM_MAX_RANGE = 30;

    @NonNull
    private final ShardInfo shardInfo;
    @NonNull
    private final ShardDetector shardDetector;
    @NonNull
    private final ShardRecordProcessor shardRecordProcessor;
    @NonNull
    private final ShardRecordProcessorCheckpointer recordProcessorCheckpointer;
    @NonNull
    private final ShutdownReason reason;
    private final ShutdownNotification shutdownNotification;
    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;
    @NonNull
    private final LeaseCoordinator leaseCoordinator;
    private final long backoffTimeMillis;
    @NonNull
    private final RecordsPublisher recordsPublisher;
    @NonNull
    private final HierarchicalShardSyncer hierarchicalShardSyncer;
    @NonNull
    private final MetricsFactory metricsFactory;

    private final TaskType taskType = TaskType.SHUTDOWN;

    private final List<ChildShard> childShards;
    @NonNull
    private final LeaseCleanupManager leaseCleanupManager;

    /*
     * Invokes ShardRecordProcessor shutdown() API.
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#call()
     */
    @Override
    public TaskResult call() {
        recordProcessorCheckpointer.checkpointer().operation(SHUTDOWN_TASK_OPERATION);
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, SHUTDOWN_TASK_OPERATION);

        final String leaseKey = ShardInfo.getLeaseKey(shardInfo);
        try {
            try {
                log.debug("Invoking shutdown() for shard {} with childShards {}, concurrencyToken {}. Shutdown reason: {}",
                        leaseKey, childShards, shardInfo.concurrencyToken(), reason);

                final long startTime = System.currentTimeMillis();
                final Lease currentShardLease = leaseCoordinator.getCurrentlyHeldLease(leaseKey);
                final Runnable leaseLostAction = () -> shardRecordProcessor.leaseLost(LEASE_LOST_INPUT);

                if (reason == ShutdownReason.SHARD_END) {
                    try {
                        takeShardEndAction(currentShardLease, leaseKey, scope, startTime);
                    } catch (InvalidStateException e) {
                        // If InvalidStateException happens, it indicates we have a non recoverable error in short term.
                        // In this scenario, we should shutdown the shardConsumer with LEASE_LOST reason to allow
                        // other worker to take the lease and retry shutting down.
                        log.warn("Lease {}: Invalid state encountered while shutting down shardConsumer with SHARD_END reason. " +
                                "Dropping the lease and shutting down shardConsumer using LEASE_LOST reason.",
                                leaseKey, e);
                        dropLease(currentShardLease, leaseKey);
                        throwOnApplicationException(leaseKey, leaseLostAction, scope, startTime);
                    }
                } else {
                    throwOnApplicationException(leaseKey, leaseLostAction, scope, startTime);
                }

                log.debug("Shutting down retrieval strategy for shard {}.", leaseKey);
                recordsPublisher.shutdown();

                // shutdownNotification is only set and used when gracefulShutdown starts
                if (shutdownNotification != null) {
                    shutdownNotification.shutdownComplete();
                }

                log.debug("Record processor completed shutdown() for shard {}", leaseKey);

                return new TaskResult(null);
            } catch (Exception e) {
                if (e instanceof CustomerApplicationException) {
                    log.error("Shard {}: Application exception.", leaseKey, e);
                } else {
                    log.error("Shard {}: Caught exception:", leaseKey, e);
                }
                // backoff if we encounter an exception.
                try {
                    Thread.sleep(this.backoffTimeMillis);
                } catch (InterruptedException ie) {
                    log.debug("Shard {}: Interrupted sleep", leaseKey, ie);
                }

                return new TaskResult(e);
            }
        } finally {
            MetricsUtil.endScope(scope);
        }
    }

    // Involves persisting child shard info, attempt to checkpoint and enqueueing lease for cleanup.
    private void takeShardEndAction(Lease currentShardLease,
            final String leaseKey, MetricsScope scope, long startTime)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException,
            CustomerApplicationException {
        // Create new lease for the child shards if they don't exist.
        // We have one valid scenario that shutdown task got created with SHARD_END reason and an empty list of childShards.
        // This would happen when KinesisDataFetcher(for polling mode) or FanOutRecordsPublisher(for StoS mode) catches ResourceNotFound exception.
        // In this case, KinesisDataFetcher and FanOutRecordsPublisher will send out SHARD_END signal to trigger a
        // shutdown task with empty list of childShards.
        // This scenario could happen when customer deletes the stream while leaving the KCL application running.
        if (currentShardLease == null) {
            throw new InvalidStateException(leaseKey
                    + " : Lease not owned by the current worker. Leaving ShardEnd handling to new owner.");
        }
        if (!CollectionUtils.isNullOrEmpty(childShards)) {
            createLeasesForChildShardsIfNotExist(scope);
            updateLeaseWithChildShards(currentShardLease);
        }
        final LeasePendingDeletion leasePendingDeletion = new LeasePendingDeletion(
                currentShardLease, shardInfo, shardDetector);
        if (!leaseCleanupManager.isEnqueuedForDeletion(leasePendingDeletion)) {
            boolean isSuccess = false;
            try {
                isSuccess = attemptShardEndCheckpointing(leaseKey, scope, startTime);
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

    private boolean attemptShardEndCheckpointing(final String leaseKey, MetricsScope scope, long startTime)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException,
            CustomerApplicationException {
        final Lease leaseFromDdb = Optional.ofNullable(leaseCoordinator.leaseRefresher().getLease(leaseKey))
                .orElseThrow(() -> new InvalidStateException("Lease for shard " + leaseKey + " does not exist."));
        if (!leaseFromDdb.checkpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
            // Call the shardRecordsProcessor to checkpoint with SHARD_END sequence number.
            // The shardEnded is implemented by customer. We should validate if the SHARD_END checkpointing is
            // successful after calling shardEnded.
            throwOnApplicationException(leaseKey, () -> applicationCheckpointAndVerification(leaseKey),
                    scope, startTime);
        }
        return true;
    }

    private void applicationCheckpointAndVerification(final String leaseKey) {
        recordProcessorCheckpointer
                .sequenceNumberAtShardEnd(recordProcessorCheckpointer.largestPermittedCheckpointValue());
        recordProcessorCheckpointer.largestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
        shardRecordProcessor.shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        final ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.lastCheckpointValue();
        if (!ExtendedSequenceNumber.SHARD_END.equals(lastCheckpointValue)) {
            throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                    + leaseKey + ". Application must checkpoint upon shard end. " +
                    "See ShardRecordProcessor.shardEnded javadocs for more information.");
        }
    }

    private void throwOnApplicationException(final String leaseKey, Runnable action, MetricsScope metricsScope,
            final long startTime)
            throws CustomerApplicationException {
        try {
            action.run();
        } catch (Exception e) {
            throw new CustomerApplicationException("Customer application throws exception for shard " + leaseKey + ": ", e);
        } finally {
            MetricsUtil.addLatency(metricsScope, RECORD_PROCESSOR_SHUTDOWN_METRIC, startTime, MetricsLevel.SUMMARY);
        }
    }

    private void createLeasesForChildShardsIfNotExist(MetricsScope scope)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final LeaseRefresher leaseRefresher = leaseCoordinator.leaseRefresher();

        // For child shard resulted from merge of two parent shards, verify if both the parents are either present or
        // not present in the lease table before creating the lease entry.
        if (childShards.size() == 1) {
            final ChildShard childShard = childShards.get(0);
            final List<String> parentLeaseKeys = childShard.parentShards().stream()
                    .map(parentShardId -> ShardInfo.getLeaseKey(shardInfo, parentShardId)).collect(Collectors.toList());
            if (parentLeaseKeys.size() != 2) {
                MetricsUtil.addCount(scope, "MissingMergeParent", 1, MetricsLevel.SUMMARY);
                throw new InvalidStateException("Shard " + shardInfo.shardId() + "'s only child shard " + childShard
                        + " does not contain other parent information.");
            }

            final Lease parentLease0 = leaseRefresher.getLease(parentLeaseKeys.get(0));
            final Lease parentLease1 = leaseRefresher.getLease(parentLeaseKeys.get(1));
            if (Objects.isNull(parentLease0) != Objects.isNull(parentLease1)) {
                MetricsUtil.addCount(scope, "MissingMergeParentLease", 1, MetricsLevel.SUMMARY);
                final String message = "Shard " + shardInfo.shardId() + "'s only child shard " + childShard +
                        " has partial parent information in lease table: [parent0=" + parentLease0 +
                        ", parent1=" + parentLease1 + "]. Hence deferring lease creation of child shard.";
                if (isOneInNProbability(RETRY_RANDOM_MAX_RANGE)) {
                    // abort further attempts and drop the lease; lease will
                    // be reassigned
                    throw new InvalidStateException(message);
                } else {
                    // initiate a Thread.sleep(...) and keep the lease;
                    // keeping the lease decreases churn of lease reassignments
                    throw new BlockedOnParentShardException(message);
                }
            }
        }

        for (ChildShard childShard : childShards) {
            final String leaseKey = ShardInfo.getLeaseKey(shardInfo, childShard.shardId());
            if (leaseRefresher.getLease(leaseKey) == null) {
                log.debug("{} - Shard {} - Attempting to create lease for child shard {}",
                        shardDetector.streamIdentifier(), shardInfo.shardId(), leaseKey);
                final Lease leaseToCreate = hierarchicalShardSyncer.createLeaseForChildShard(childShard, shardDetector.streamIdentifier());
                final long startTime = System.currentTimeMillis();
                boolean success = false;
                try {
                    leaseRefresher.createLeaseIfNotExists(leaseToCreate);
                    success = true;
                } finally {
                    MetricsUtil.addSuccessAndLatency(scope, "CreateLease", success, startTime, MetricsLevel.DETAILED);
                    if (leaseToCreate.checkpoint() != null) {
                        final String metricName = leaseToCreate.checkpoint().isSentinelCheckpoint() ?
                                leaseToCreate.checkpoint().sequenceNumber() : "SEQUENCE_NUMBER";
                        MetricsUtil.addSuccess(scope, "CreateLease_" + metricName, true, MetricsLevel.DETAILED);
                    }
                }

                log.info("{} - Shard {}: Created child shard lease: {}", shardDetector.streamIdentifier(), shardInfo.shardId(), leaseToCreate);
            }
        }
    }

    /**
     * Returns true for 1 in N probability.
     */
    @VisibleForTesting
    boolean isOneInNProbability(int n) {
        return 0 == RANDOM.nextInt(n);
    }

    private void updateLeaseWithChildShards(Lease currentLease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Set<String> childShardIds = childShards.stream().map(ChildShard::shardId).collect(Collectors.toSet());

        final Lease updatedLease = currentLease.copy();
        updatedLease.childShardIds(childShardIds);
        leaseCoordinator.leaseRefresher().updateLeaseWithMetaInfo(updatedLease, UpdateField.CHILD_SHARDS);
        log.info("Shard {}: Updated current lease {} with child shard information: {}", shardInfo.shardId(), currentLease.leaseKey(), childShardIds);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#taskType()
     */
    @Override
    public TaskType taskType() {
        return taskType;
    }

    @VisibleForTesting
    public ShutdownReason getReason() {
        return reason;
    }

    private void dropLease(Lease currentLease, final String leaseKey) {
        if (currentLease == null) {
            log.warn("Shard {}: Unable to find the lease for shard. Will shutdown the shardConsumer directly.", leaseKey);
        } else {
            leaseCoordinator.dropLease(currentLease);
            log.info("Dropped lease for shutting down ShardConsumer: " + currentLease.leaseKey());
        }
    }
}
