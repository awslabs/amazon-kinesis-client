/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * This class handles the graceful shutdown of shard consumers. When a lease is requested for shutdown, it will be
 * enqueued from the lease renewal thread which will call the shard consumer of the lease to enqueue a shutdown request.
 * The class monitors those leases and check if the shutdown is properly completed.
 * If the shard consumer doesn't shut down within the given timeout, it will trigger a lease transfer.
 */
@Slf4j
@RequiredArgsConstructor
@KinesisClientInternalApi
public class LeaseGracefulShutdownHandler {

    // Arbitrary number to run a similar frequency as the scheduler based on shardConsumerDispatchPollIntervalMillis
    // which is how fast scheduler triggers state change. It's ok to add few extra second delay to call shutdown since
    // the leases should still be processing by the current owner so there should not be processing delay due to this.
    private static final long SHUTDOWN_CHECK_INTERVAL_MILLIS = 2000;

    private final long shutdownTimeoutMillis;
    private final ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap;
    private final LeaseCoordinator leaseCoordinator;
    private final Supplier<Long> currentTimeSupplier;
    private final ConcurrentMap<ShardInfo, LeasePendingShutdown> shardInfoLeasePendingShutdownMap =
            new ConcurrentHashMap<>();
    private final ScheduledExecutorService executorService;

    private volatile boolean isRunning = false;

    /**
     * Factory method to create a new instance of LeaseGracefulShutdownHandler.
     *
     * @param shutdownTimeoutMillis     Timeout for graceful shutdown of shard consumers.
     * @param shardInfoShardConsumerMap Map of shard info to shard consumer instances.
     * @param leaseCoordinator          Lease coordinator instance to access lease information.
     * @return A new instance of LeaseGracefulShutdownHandler.
     */
    public static LeaseGracefulShutdownHandler create(
            long shutdownTimeoutMillis,
            ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap,
            LeaseCoordinator leaseCoordinator) {
        return new LeaseGracefulShutdownHandler(
                shutdownTimeoutMillis,
                shardInfoShardConsumerMap,
                leaseCoordinator,
                System::currentTimeMillis,
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                        .setNameFormat("LeaseGracefulShutdown-%04d")
                        .setDaemon(true)
                        .build()));
    }

    /**
     * Starts the shard consumer shutdown handler thread.
     */
    public void start() {
        if (!isRunning) {
            log.info("Starting graceful lease handoff thread.");
            executorService.scheduleAtFixedRate(
                    this::monitorGracefulShutdownLeases, 0, SHUTDOWN_CHECK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            isRunning = true;
        } else {
            log.info("Graceful lease handoff thread already running, no need to start.");
        }
    }

    /**
     * Stops the shard consumer shutdown handler thread.
     */
    public void stop() {
        if (isRunning) {
            log.info("Stopping graceful lease handoff thread.");
            executorService.shutdown();
            isRunning = false;
        } else {
            log.info("Graceful lease handoff thread already stopped.");
        }
    }

    /**
     * Enqueue a shutdown request for the given lease if the lease has requested shutdown and the shard consumer
     * is not already shutdown.
     *
     * @param lease The lease to enqueue a shutdown request for.
     */
    public void enqueueShutdown(Lease lease) {
        if (lease == null || !lease.shutdownRequested() || !isRunning) {
            return;
        }
        final ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
        final ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);
        if (consumer == null || consumer.isShutdown()) {
            shardInfoLeasePendingShutdownMap.remove(shardInfo);
        } else {
            // there could be change shard get enqueued after getting removed. This should be okay because
            // this enqueue will be no-op and will be removed again because the shardConsumer associated with the
            // shardInfo is shutdown by then.
            shardInfoLeasePendingShutdownMap.computeIfAbsent(shardInfo, key -> {
                log.info("Calling graceful shutdown for lease {}", lease.leaseKey());
                LeasePendingShutdown leasePendingShutdown = new LeasePendingShutdown(lease, consumer);
                initiateShutdown(leasePendingShutdown);
                return leasePendingShutdown;
            });
        }
    }

    /**
     * Wait for shutdown to complete or transfer ownership of lease to the next owner if timeout is met.
     */
    private void monitorGracefulShutdownLeases() {
        String leaseKey = null;
        try {
            for (ConcurrentMap.Entry<ShardInfo, LeasePendingShutdown> entry :
                    shardInfoLeasePendingShutdownMap.entrySet()) {
                final LeasePendingShutdown leasePendingShutdown = entry.getValue();
                final ShardInfo shardInfo = entry.getKey();
                leaseKey = leasePendingShutdown.lease.leaseKey();

                if (leasePendingShutdown.shardConsumer.isShutdown()
                        || shardInfoShardConsumerMap.get(shardInfo) == null
                        || leaseCoordinator.getCurrentlyHeldLease(leaseKey) == null) {
                    logTimeoutMessage(leasePendingShutdown);
                    shardInfoLeasePendingShutdownMap.remove(shardInfo);
                } else if (getCurrentTimeMillis() >= leasePendingShutdown.timeoutTimestampMillis
                        && !leasePendingShutdown.leaseTransferCalled) {
                    try {
                        log.info(
                                "Timeout {} millisecond reached waiting for lease {} to graceful handoff."
                                        + " Attempting to transfer the lease to {}",
                                shutdownTimeoutMillis,
                                leaseKey,
                                leasePendingShutdown.lease.leaseOwner());
                        transferLeaseIfOwner(leasePendingShutdown);
                    } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
                        log.warn("Failed to transfer lease for key {}. Will retry", leaseKey, e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error in graceful shutdown for lease {}", leaseKey, e);
        }
    }

    private void initiateShutdown(LeasePendingShutdown tracker) {
        tracker.shardConsumer.gracefulShutdown(null);
        tracker.shutdownRequested = true;
        tracker.timeoutTimestampMillis = getCurrentTimeMillis() + shutdownTimeoutMillis;
    }

    private void logTimeoutMessage(LeasePendingShutdown leasePendingShutdown) {
        if (leasePendingShutdown.leaseTransferCalled) {
            final long timeElapsedSinceShutdownInitiated =
                    getCurrentTimeMillis() - leasePendingShutdown.timeoutTimestampMillis + shutdownTimeoutMillis;
            log.info(
                    "Lease {} took {} milliseconds to complete the shutdown. "
                            + "Consider tuning the GracefulLeaseHandoffTimeoutMillis to prevent timeouts, "
                            + "if necessary.",
                    leasePendingShutdown.lease.leaseKey(),
                    timeElapsedSinceShutdownInitiated);
        }
    }

    private void transferLeaseIfOwner(LeasePendingShutdown leasePendingShutdown)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final Lease lease = leasePendingShutdown.lease;
        if (leaseCoordinator.workerIdentifier().equals(lease.checkpointOwner())) {
            // assignLease will increment the leaseCounter which will cause the heartbeat to stop on the current owner
            // for the lease
            leaseCoordinator.leaseRefresher().assignLease(lease, lease.leaseOwner());
        } else {
            // the worker ID check is just for sanity. We don't expect it to be different from the current worker.
            log.error(
                    "Lease {} checkpoint owner mismatch found {} but it should be {}",
                    lease.leaseKey(),
                    lease.checkpointOwner(),
                    leaseCoordinator.workerIdentifier());
        }
        // mark it true because we don't want to enter the method again because update is not possible anymore.
        leasePendingShutdown.leaseTransferCalled = true;
    }

    private long getCurrentTimeMillis() {
        return currentTimeSupplier.get();
    }

    @Data
    private static class LeasePendingShutdown {
        final Lease lease;
        final ShardConsumer shardConsumer;
        long timeoutTimestampMillis;
        boolean shutdownRequested = false;
        boolean leaseTransferCalled = false;
    }
}
