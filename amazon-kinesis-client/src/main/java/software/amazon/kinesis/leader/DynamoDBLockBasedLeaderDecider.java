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

package software.amazon.kinesis.leader;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.segmenting.FleetSegmentingHandler;

/**
 * Implementation for LeaderDecider to elect leader using lock on dynamo db table. This class uses
 * AmazonDynamoDBLockClient library to perform the leader election.
 */
@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLockBasedLeaderDecider implements LeaderDecider {

    private final CoordinatorStateDAO coordinatorStateDao;
    private final AmazonDynamoDBLockClient dynamoDBLockClient;
    private final Long heartbeatPeriodMillis;
    private final String workerId;
    private final MetricsFactory metricsFactory;
    private final FleetSegmentingHandler segmentingHandler;

    private long lastCheckTimeInMillis = 0L;
    private boolean lastIsLeaderResult = false;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    @VisibleForTesting
    static DynamoDBLockBasedLeaderDecider create(
            final CoordinatorStateDAO coordinatorStateDao,
            final String workerId,
            final Long leaseDuration,
            final Long heartbeatPeriod,
            final MetricsFactory metricsFactory,
            final FleetSegmentingHandler segmentingHandler) {
        final AmazonDynamoDBLockClient dynamoDBLockClient = new AmazonDynamoDBLockClient(coordinatorStateDao
                .getDDBLockClientOptionsBuilder()
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withLeaseDuration(leaseDuration)
                .withHeartbeatPeriod(heartbeatPeriod)
                .withCreateHeartbeatBackgroundThread(true)
                .withOwnerName(workerId)
                .build());

        return new DynamoDBLockBasedLeaderDecider(
                coordinatorStateDao, dynamoDBLockClient, heartbeatPeriod, workerId, metricsFactory, segmentingHandler);
    }

    public static DynamoDBLockBasedLeaderDecider create(
            final CoordinatorStateDAO coordinatorStateDao,
            final String workerId,
            final MetricsFactory metricsFactory,
            long leaseDurationInMillis,
            long heartbeatPeriodInMillis,
            final FleetSegmentingHandler segmentingHandler) {
        return create(
                coordinatorStateDao,
                workerId,
                leaseDurationInMillis,
                heartbeatPeriodInMillis,
                metricsFactory,
                segmentingHandler);
    }

    @Override
    public void initialize() {
        log.info("Initializing DDB Lock based leader decider");
    }

    /**
     * Check the lockItem in storage and if the current worker is not leader worker, then tries to acquire lock and
     * returns true if it was able to acquire lock else false.
     * @param workerId ID of the worker
     * @return true if current worker is leader else false.
     */
    @Override
    public synchronized Boolean isLeader(final String workerId) {
        // if the decider has shutdown, then return false and don't try acquireLock anymore.
        if (isShutdown.get()) {
            publishIsLeaderMetrics(false);
            return false;
        }
        // If the last time we tried to take lock and didnt get lock, don't try to take again for heartbeatPeriodMillis
        // this is to avoid unnecessary calls to dynamoDB.
        // Different modules in KCL can request for isLeader check within heartbeatPeriodMillis, and this optimization
        // will help in those cases.
        // In case the last call returned true, we want to check the source always to ensure the correctness of leader.
        if (!lastIsLeaderResult && lastCheckTimeInMillis + heartbeatPeriodMillis > System.currentTimeMillis()) {
            publishIsLeaderMetrics(lastIsLeaderResult);
            return lastIsLeaderResult;
        }
        boolean response;

        // If this worker is the deploying leader and all workers are emitting the deploying version, try to acquire
        // the current leader lock. The worker has to be the deploying leader so that it can release its lock.
        // Otherwise, two leaders on the same version hash may be functioning at the same time
        final String ddbLeaderKey;
        ddbLeaderKey = segmentingHandler.getHashKeyForLeaderLock();

        // Get the lockItem from storage (if present)
        final Optional<LockItem> lockItem = dynamoDBLockClient.getLock(ddbLeaderKey, Optional.empty());
        lockItem.ifPresent(item -> log.info("Worker : {} is the current {}.", item.getOwnerName(), ddbLeaderKey));

        // If the lockItem is present and is expired, that means either current worker is not leader.
        if (!lockItem.isPresent() || lockItem.get().isExpired()) {
            try {
                // Current worker does not hold the lock, try to acquireOne.
                final Optional<LockItem> leaderLockItem = dynamoDBLockClient.tryAcquireLock(AcquireLockOptions.builder(
                                ddbLeaderKey)
                        .withRefreshPeriod(heartbeatPeriodMillis)
                        .withTimeUnit(TimeUnit.MILLISECONDS)
                        .withShouldSkipBlockingWait(true)
                        .withAdditionalAttributes(segmentingHandler.getVersionHashWithLastUpdatedTimeForLockTable())
                        .build());
                leaderLockItem.ifPresent(item -> log.info("Worker : {} is new leader", item.getOwnerName()));
                // if leaderLockItem optional is empty, that means the lock is not acquired by this worker.
                response = leaderLockItem.isPresent();
            } catch (final InterruptedException e) {
                // Something bad happened, don't assume leadership and also release lock just in case the
                // lock was granted and still interrupt happened.
                releaseLeadershipIfHeld();
                log.error("Acquiring lock was interrupted in between", e);
                response = false;

            } catch (final LockCurrentlyUnavailableException e) {
                response = false;
            }

        } else {
            response = lockItem.get().getOwnerName().equals(workerId);
        }

        lastCheckTimeInMillis = System.currentTimeMillis();
        lastIsLeaderResult = response;
        publishIsLeaderMetrics(response);

        // only release the deploying leader lock if this worker also acquired the current leader lock
        if (isWorkerDeployingLeader()
                && isWorkerCurrentLeader()
                && segmentingHandler.isVersionEmittedByAllActiveWorkers()) {
            releaseDeployingLeaderLock();
        }

        return response;
    }

    private boolean isWorkerDeployingLeader() {
        Optional<LockItem> lockItem =
                dynamoDBLockClient.getLock(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, Optional.empty());
        return lockItem.isPresent() && lockItem.get().getOwnerName().equals(workerId);
    }

    private boolean isWorkerCurrentLeader() {
        Optional<LockItem> lockItem = dynamoDBLockClient.getLock(CoordinatorState.LEADER_HASH_KEY, Optional.empty());
        return lockItem.isPresent() && lockItem.get().getOwnerName().equals(workerId);
    }

    private void releaseDeployingLeaderLock() {
        final Optional<LockItem> lockItem =
                dynamoDBLockClient.getLock(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, Optional.empty());
        if (lockItem.isPresent()
                && !lockItem.get().isExpired()
                && lockItem.get().getOwnerName().equals(workerId)) {
            log.info("Releasing deploying leader lock since all workers are emitting the deploying version.");
            lockItem.get().close();
        }
    }

    private void publishIsLeaderMetrics(final boolean response) {
        final MetricsScope metricsScope =
                MetricsUtil.createMetricsWithOperation(metricsFactory, METRIC_OPERATION_LEADER_DECIDER);
        metricsScope.addData(
                METRIC_OPERATION_LEADER_DECIDER_IS_LEADER, response ? 1 : 0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        MetricsUtil.endScope(metricsScope);
    }

    /**
     * Shuts down the leader decider, releasing any held lock and closing the lock client.
     * <p>
     * This method releases the leadership lock if held and then closes the underlying
     * DynamoDB lock client to stop its background heartbeat thread. This ensures that
     * no locks are kept alive after shutdown.
     */
    @Override
    public synchronized void shutdown() {
        if (!isShutdown.getAndSet(true)) {
            releaseLeadershipIfHeld();

            // Close the any lock client to stop any potential background heartbeat thread.
            try {
                log.info("Closing DynamoDB lock client for worker {}", workerId);
                dynamoDBLockClient.close();
            } catch (final IOException e) {
                log.error("Failed to close DynamoDB lock client for worker {}", workerId, e);
            }
        }
    }

    @Override
    public synchronized void releaseLeadershipIfHeld() {
        try {
            final String ddbLeaderKey = segmentingHandler.getHashKeyForLeaderLock();
            final Optional<LockItem> lockItem = dynamoDBLockClient.getLock(ddbLeaderKey, Optional.empty());
            if (lockItem.isPresent()
                    && !lockItem.get().isExpired()
                    && lockItem.get().getOwnerName().equals(workerId)) {

                log.info(
                        "Current worker : {} holds the lock, releasing it.",
                        lockItem.get().getOwnerName());
                // LockItem.close() will release the lock if current worker owns it else this call is no op.
                lockItem.get().close();
            }
        } catch (final Exception e) {
            log.error("Failed to complete releaseLeadershipIfHeld call.", e);
        }
    }
}
