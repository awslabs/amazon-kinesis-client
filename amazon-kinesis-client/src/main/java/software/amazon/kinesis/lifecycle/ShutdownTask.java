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
import java.util.Optional;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
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

import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
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
    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;
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
    private final StreamIdentifier streamIdentifier;
    @NonNull
    private final LeaseCleanupManager leaseCleanupManager;

    private static final Function<ShardInfo, String> leaseKeyProvider = shardInfo -> ShardInfo.getLeaseKey(shardInfo);

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

        Exception exception;

        try {
            try {
                log.debug("Invoking shutdown() for shard {}, concurrencyToken {}. Shutdown reason: {}",
                        leaseKeyProvider.apply(shardInfo), shardInfo.concurrencyToken(), reason);

                final long startTime = System.currentTimeMillis();
                if (reason == ShutdownReason.SHARD_END) {
                    // Create new lease for the child shards if they don't exist.
                    // We have one valid scenario that shutdown task got created with SHARD_END reason and an empty list of childShards.
                    // This would happen when KinesisDataFetcher(for polling mode) or FanOutRecordsPublisher(for StoS mode) catches ResourceNotFound exception.
                    // In this case, KinesisDataFetcher and FanOutRecordsPublisher will send out SHARD_END signal to trigger a shutdown task with empty list of childShards.
                    // This scenario could happen when customer deletes the stream while leaving the KCL application running.
                    final Lease currentShardLease = leaseCoordinator.getCurrentlyHeldLease(leaseKeyProvider.apply(shardInfo));
                    final LeasePendingDeletion leasePendingDeletion = new LeasePendingDeletion(streamIdentifier,
                            currentShardLease, shardInfo);

                    if (!CollectionUtils.isNullOrEmpty(childShards)) {
                        createLeasesForChildShardsIfNotExist();
                        updateLeaseWithChildShards(currentShardLease);
                        // Attempt to do shard checkpointing and throw on exception.
                        attemptShardEndCheckpointing(scope, startTime);
                        // Enqueue completed shard for deletion.
                        leaseCleanupManager.enqueueForDeletion(leasePendingDeletion);

                    } else {
                        // This might be a case of ResourceNotFound from Service. Directly validate and delete lease, if required.
                        // If already enqueued for deletion as part of this worker, do not attempt to shard end checkpoint
                        // or lease cleanup. Else try to shard end checkpoint and cleanup the lease if the shard is a
                        // garbage shard.
                        if (!leaseCleanupManager.isEnqueuedForDeletion(leasePendingDeletion)) {
                            try {
                                // Do a best effort shard end checkpointing, before attempting to cleanup the lease,
                                // in the case of RNF Exception.
                                attemptShardEndCheckpointing(scope, startTime);
                            } finally {
                                // If we don't want to cleanup the garbage shard without successful shard end
                                // checkpointing, remove the try finally construct and only execute the methods.
                                attemptGarbageCollectionOfLeaseAndEnqueueOnFailure(leasePendingDeletion, currentShardLease);
                            }
                        }
                    }
                } else {
                    throwOnApplicationException(() -> shardRecordProcessor.leaseLost(LeaseLostInput.builder().build()), scope, startTime);
                }

                log.debug("Shutting down retrieval strategy for shard {}.", leaseKeyProvider.apply(shardInfo));
                recordsPublisher.shutdown();
                log.debug("Record processor completed shutdown() for shard {}", leaseKeyProvider.apply(shardInfo));

                return new TaskResult(null);
            } catch (Exception e) {
                if (e instanceof CustomerApplicationException) {
                    log.error("Shard {}: Application exception. ", leaseKeyProvider.apply(shardInfo), e);
                } else {
                    log.error("Shard {}: Caught exception: ", leaseKeyProvider.apply(shardInfo), e);
                }
                exception = e;
                // backoff if we encounter an exception.
                try {
                    Thread.sleep(this.backoffTimeMillis);
                } catch (InterruptedException ie) {
                    log.debug("Shard {}: Interrupted sleep", leaseKeyProvider.apply(shardInfo), ie);
                }
            }
        } finally {
            MetricsUtil.endScope(scope);
        }

        return new TaskResult(exception);
    }

    private void attemptShardEndCheckpointing(MetricsScope scope, long startTime)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException,
            CustomerApplicationException {
        final Lease leaseFromDdb = Optional.ofNullable(leaseCoordinator.leaseRefresher().getLease(leaseKeyProvider.apply(shardInfo)))
                .orElseThrow(() -> new IllegalStateException("Lease for shard " + leaseKeyProvider.apply(shardInfo) + " does not exist."));
        if (!leaseFromDdb.checkpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
            // Call the shardRecordsProcessor to checkpoint with SHARD_END sequence number.
            // The shardEnded is implemented by customer. We should validate if the SHARD_END checkpointing is successful after calling shardEnded.
            throwOnApplicationException(() -> applicationCheckpointAndVerification(), scope, startTime);
        }
    }

    private void attemptGarbageCollectionOfLeaseAndEnqueueOnFailure(LeasePendingDeletion leasePendingDeletion, Lease currentShardLease) {
        final LeaseCleanupManager.LeaseCleanupResult leaseCleanupResult;
        try {
            leaseCleanupResult = leaseCleanupManager
                    .cleanupLease(leasePendingDeletion, false, true);
            if (leaseCleanupResult.leaseCleanedUp()) {
                log.info("Cleaned up garbage lease {} for {}. Details : {}",
                        currentShardLease.leaseKey(), streamIdentifier, leaseCleanupResult);
            } else {
                log.error("Unable to cleanup potential garbage lease {} for {}. Details : {} ",
                        currentShardLease.leaseKey(), streamIdentifier, leaseCleanupResult);
                // If we are unable to delete this lease and the reason being RNF, then enqueue it
                // for deletion, so that we don't end up consuming service TPS on any bugs.
                if (leaseCleanupResult.wasResourceNotFound()) {
                    leaseCleanupManager.enqueueForDeletion(leasePendingDeletion);
                }
            }
        } catch (Exception e) {
            log.error("Unable to cleanup potential garbage lease {} for {}", currentShardLease.leaseKey(),
                    streamIdentifier, e);
        }
    }

    private void applicationCheckpointAndVerification() {
        recordProcessorCheckpointer
                .sequenceNumberAtShardEnd(recordProcessorCheckpointer.largestPermittedCheckpointValue());
        recordProcessorCheckpointer.largestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
        shardRecordProcessor.shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        final ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.lastCheckpointValue();
        if (lastCheckpointValue == null
                || !lastCheckpointValue.equals(ExtendedSequenceNumber.SHARD_END)) {
            throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                    + leaseKeyProvider.apply(shardInfo) + ". Application must checkpoint upon shard end. " +
                    "See ShardRecordProcessor.shardEnded javadocs for more information.");
        }
    }

    private void throwOnApplicationException(Runnable action, MetricsScope metricsScope, final long startTime) throws CustomerApplicationException {
        try {
            action.run();
        } catch (Exception e) {
            throw new CustomerApplicationException("Customer application throws exception for shard " + leaseKeyProvider.apply(shardInfo) +": ", e);
        } finally {
            MetricsUtil.addLatency(metricsScope, RECORD_PROCESSOR_SHUTDOWN_METRIC, startTime, MetricsLevel.SUMMARY);
        }
    }

    private void createLeasesForChildShardsIfNotExist()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        for(ChildShard childShard : childShards) {
            final String leaseKey = ShardInfo.getLeaseKey(shardInfo, childShard.shardId());
            if(leaseCoordinator.getCurrentlyHeldLease(leaseKey) == null) {
                final Lease leaseToCreate = hierarchicalShardSyncer.createLeaseForChildShard(childShard, shardDetector.streamIdentifier());
                leaseCoordinator.leaseRefresher().createLeaseIfNotExists(leaseToCreate);
                log.info("Shard {}: Created child shard lease: {}", shardInfo.shardId(), leaseToCreate.leaseKey());
            }
        }
    }

    private void updateLeaseWithChildShards(Lease currentLease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Set<String> childShardIds = childShards.stream().map(ChildShard::shardId).collect(Collectors.toSet());

        final Lease updatedLease = currentLease.copy();
        updatedLease.childShardIds(childShardIds);
        // TODO : Make changes to use the new leaserefresher#updateLease(Lease lease, UpdateField updateField)
        final boolean updateResult = leaseCoordinator.updateLease(updatedLease, UUID.fromString(shardInfo.concurrencyToken()), SHUTDOWN_TASK_OPERATION, leaseKeyProvider.apply(shardInfo));
        if (!updateResult) {
            throw new InvalidStateException("Failed to update parent lease with child shard information for shard " + shardInfo.shardId());
        }
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

}
