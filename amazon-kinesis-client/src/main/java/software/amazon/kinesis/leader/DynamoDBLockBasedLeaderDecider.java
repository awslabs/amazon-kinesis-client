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

import java.time.Duration;
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
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static software.amazon.kinesis.coordinator.CoordinatorState.LEADER_HASH_KEY;

/**
 * Implementation for LeaderDecider to elect leader using lock on dynamo db table. This class uses
 * AmazonDynamoDBLockClient library to perform the leader election.
 */
@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLockBasedLeaderDecider implements LeaderDecider {
    private static final Long DEFAULT_LEASE_DURATION_MILLIS =
            Duration.ofMinutes(2).toMillis();
    // Heartbeat frequency should be at-least 3 times smaller the lease duration according to LockClient documentation
    private static final Long DEFAULT_HEARTBEAT_PERIOD_MILLIS =
            Duration.ofSeconds(30).toMillis();

    private final CoordinatorStateDAO coordinatorStateDao;
    private final AmazonDynamoDBLockClient dynamoDBLockClient;
    private final Long heartbeatPeriodMillis;
    private final String workerId;
    private final MetricsFactory metricsFactory;

    private long lastCheckTimeInMillis = 0L;
    private boolean lastIsLeaderResult = false;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    @VisibleForTesting
    static DynamoDBLockBasedLeaderDecider create(
            final CoordinatorStateDAO coordinatorStateDao,
            final String workerId,
            final Long leaseDuration,
            final Long heartbeatPeriod,
            final MetricsFactory metricsFactory) {
        final AmazonDynamoDBLockClient dynamoDBLockClient = new AmazonDynamoDBLockClient(coordinatorStateDao
                .getDDBLockClientOptionsBuilder()
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withLeaseDuration(leaseDuration)
                .withHeartbeatPeriod(heartbeatPeriod)
                .withCreateHeartbeatBackgroundThread(true)
                .withOwnerName(workerId)
                .build());

        return new DynamoDBLockBasedLeaderDecider(
                coordinatorStateDao, dynamoDBLockClient, heartbeatPeriod, workerId, metricsFactory);
    }

    public static DynamoDBLockBasedLeaderDecider create(
            final CoordinatorStateDAO coordinatorStateDao, final String workerId, final MetricsFactory metricsFactory) {
        return create(
                coordinatorStateDao,
                workerId,
                DEFAULT_LEASE_DURATION_MILLIS,
                DEFAULT_HEARTBEAT_PERIOD_MILLIS,
                metricsFactory);
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
        // Get the lockItem from storage (if present
        final Optional<LockItem> lockItem = dynamoDBLockClient.getLock(LEADER_HASH_KEY, Optional.empty());
        lockItem.ifPresent(item -> log.info("Worker : {} is the current leader.", item.getOwnerName()));

        // If the lockItem is present and is expired, that means either current worker is not leader.
        if (!lockItem.isPresent() || lockItem.get().isExpired()) {
            try {
                // Current worker does not hold the lock, try to acquireOne.
                final Optional<LockItem> leaderLockItem =
                        dynamoDBLockClient.tryAcquireLock(AcquireLockOptions.builder(LEADER_HASH_KEY)
                                .withRefreshPeriod(heartbeatPeriodMillis)
                                .withTimeUnit(TimeUnit.MILLISECONDS)
                                .withShouldSkipBlockingWait(true)
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
        return response;
    }

    private void publishIsLeaderMetrics(final boolean response) {
        final MetricsScope metricsScope =
                MetricsUtil.createMetricsWithOperation(metricsFactory, METRIC_OPERATION_LEADER_DECIDER);
        metricsScope.addData(
                METRIC_OPERATION_LEADER_DECIDER_IS_LEADER, response ? 1 : 0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        MetricsUtil.endScope(metricsScope);
    }

    /**
     * Releases the lock if held by current worker when this method is invoked.
     */
    @Override
    public void shutdown() {
        if (!isShutdown.getAndSet(true)) {
            releaseLeadershipIfHeld();
        }
    }

    @Override
    public void releaseLeadershipIfHeld() {
        try {
            final Optional<LockItem> lockItem = dynamoDBLockClient.getLock(LEADER_HASH_KEY, Optional.empty());
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
