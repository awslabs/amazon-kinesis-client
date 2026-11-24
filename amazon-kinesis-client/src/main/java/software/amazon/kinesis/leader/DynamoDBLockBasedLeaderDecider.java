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

import java.util.HashMap;
import java.util.Map;
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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.coordinator.migration.TableMigrationStateMachine;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.segmenting.FleetSegmentingHandler;
import software.amazon.kinesis.segmenting.VersionStateUnavailableException;

/**
 * Implementation for LeaderDecider to elect leader using lock on dynamo db table. This class uses
 * AmazonDynamoDBLockClient library to perform the leader election.
 *
 * <p>The lock client is obtained from {@link CoordinatorStateDAO#getDDBLockClient()} on each
 * {@code isLeader()} call, which routes to the appropriate table based on the current
 * {@link software.amazon.kinesis.coordinator.migration.TableMigrationStatus}. When migration
 * completes (PENDING → COMPLETE), the DAO transparently switches from the legacy table lock
 * client to the lease table lock client.</p>
 */
@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLockBasedLeaderDecider implements LeaderDecider {

    private final CoordinatorStateDAO coordinatorStateDAO;
    private final Long heartbeatPeriodMillis;
    private final String workerId;
    private final MetricsFactory metricsFactory;
    private final TableMigrationStateMachine tableMigrationStateMachine;
    private final FleetSegmentingHandler segmentingHandler;

    private long lastCheckTimeInMillis = 0L;
    private boolean lastIsLeaderResult = false;

    // Leader lock key this worker holds (Leader or DeployingLeader), or null. Guarded by this object's monitor.
    private String heldLeaderLockKey = null;

    // When heldLeaderLockKey was last re-evaluated against the coordinator table.
    private long lastKeyEvaluationTimeMillis = 0L;
    // Whether segmenting was enabled when heldLeaderLockKey was last evaluated. Guarded by this object's monitor.
    private boolean keyEvaluatedWithSegmentingEnabled = false;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    @VisibleForTesting
    static DynamoDBLockBasedLeaderDecider create(
            final CoordinatorStateDAO coordinatorStateDao,
            final String workerId,
            final Long leaseDuration,
            final Long heartbeatPeriod,
            final MetricsFactory metricsFactory,
            final TableMigrationStateMachine tableMigrationStateMachine,
            final FleetSegmentingHandler segmentingHandler) {
        coordinatorStateDao.initializeLockClients(leaseDuration, heartbeatPeriod, workerId);

        return new DynamoDBLockBasedLeaderDecider(
                coordinatorStateDao,
                heartbeatPeriod,
                workerId,
                metricsFactory,
                tableMigrationStateMachine,
                segmentingHandler);
    }

    public static DynamoDBLockBasedLeaderDecider create(
            final CoordinatorStateDAO coordinatorStateDao,
            final String workerId,
            final MetricsFactory metricsFactory,
            long leaseDurationInMillis,
            long heartbeatPeriodInMillis,
            final TableMigrationStateMachine tableMigrationStateMachine,
            final FleetSegmentingHandler segmentingHandler) {
        return create(
                coordinatorStateDao,
                workerId,
                leaseDurationInMillis,
                heartbeatPeriodInMillis,
                metricsFactory,
                tableMigrationStateMachine,
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
        // If the last call returned true, the held lock is checked below from the lock client's local state.
        if (!lastIsLeaderResult && lastCheckTimeInMillis + heartbeatPeriodMillis > System.currentTimeMillis()) {
            publishIsLeaderMetrics(lastIsLeaderResult);
            return lastIsLeaderResult;
        }

        // Get the lock client once for this entire cycle. This ensures that if handleLeaderLockResult
        // updates the migration status to COMPLETE (flipping the DAO routing), we still use the same
        // client for releaseLeadershipIfHeld within this cycle.
        final AmazonDynamoDBLockClient lockClient = coordinatorStateDAO.getDDBLockClient();
        boolean response;

        // Which lock to contend for. A worker that already holds a lock keeps using that key and only re-evaluates
        // it once per heartbeat period. getTargetLeaderLockKey() reads the coordinator table, and isLeader() runs
        // about once a second on the leader, so re-evaluating on every call made the leader's leadership depend on
        // coordinator-table reads.
        //
        // Whether segmenting is enabled is read once here and used for both the key and the recorded result, and a
        // change in it forces the key to be re-evaluated. Segmenting can become enabled while this runs (e.g. table
        // migration reaching COMPLETE). Reading it twice let a worker choose "Leader" while disabled (no version
        // read) and then record it as enabled, writing its version to LeaderVersion while the old version still ran.
        final long now = System.currentTimeMillis();
        final boolean segmentingEnabled = segmentingHandler.isEnabled();
        final String targetLeaderLockKey;
        if (heldLeaderLockKey != null
                && lastKeyEvaluationTimeMillis + heartbeatPeriodMillis > now
                && keyEvaluatedWithSegmentingEnabled == segmentingEnabled) {
            targetLeaderLockKey = heldLeaderLockKey;
        } else {
            try {
                targetLeaderLockKey = segmentingHandler.getTargetLeaderLockKey(segmentingEnabled);
            } catch (final VersionStateUnavailableException e) {
                // The version state has never been read successfully, so this worker can't know which lock it is
                // eligible for. Contend for neither lock this cycle rather than guessing "Leader". This worker can't
                // hold a lock here: acquiring one required a successful read.
                log.warn("Not contending for leadership this cycle: {}", e.getMessage(), e);
                recordResult(null, false, segmentingEnabled);
                return false;
            }
            lastKeyEvaluationTimeMillis = now;
            keyEvaluatedWithSegmentingEnabled = segmentingEnabled;
        }

        // If the key changed while holding a lock (e.g. the DeployingLeader at the end of a deploy, or a former
        // current leader whose version is no longer current), try the new lock first and release the held lock only
        // once the new one is held. If the new lock can't be acquired, this worker keeps the lock it holds and its
        // role, and tries again at the next key re-evaluation, instead of ending up with neither lock.
        final String previouslyHeldLockKey = heldLeaderLockKey;
        final boolean switchingLocks =
                previouslyHeldLockKey != null && !previouslyHeldLockKey.equals(targetLeaderLockKey);
        if (switchingLocks) {
            log.info(
                    "Leader lock key changed from {} to {}; acquiring {} before releasing {}.",
                    previouslyHeldLockKey,
                    targetLeaderLockKey,
                    targetLeaderLockKey,
                    previouslyHeldLockKey);
        }

        // For a lock this worker holds, getLock is answered from the lock client's local map without I/O.
        final Optional<LockItem> lockItem = lockClient.getLock(targetLeaderLockKey, Optional.empty());
        lockItem.ifPresent(
                item -> log.debug("Worker : {} is the current {}.", item.getOwnerName(), targetLeaderLockKey));

        boolean interrupted = false;
        // If the lockItem is present and is expired, that means either current worker is not leader.
        if (!lockItem.isPresent() || lockItem.get().isExpired()) {
            try {
                // Current worker does not hold the lock, try to acquireOne. The lock carries no version; the
                // segmenting handler writes this worker's version to the lock's version item once it is held.
                final Map<String, AttributeValue> lockAttributes = getLockAttributes();
                final Optional<LockItem> leaderLockItem =
                        lockClient.tryAcquireLock(AcquireLockOptions.builder(targetLeaderLockKey)
                                .withRefreshPeriod(heartbeatPeriodMillis)
                                .withTimeUnit(TimeUnit.MILLISECONDS)
                                .withShouldSkipBlockingWait(true)
                                .withAdditionalAttributes(lockAttributes)
                                .build());
                leaderLockItem.ifPresent(
                        item -> log.info("Worker : {} is new {}", item.getOwnerName(), targetLeaderLockKey));
                // if leaderLockItem optional is empty, that means the lock is not acquired by this worker.
                response = leaderLockItem.isPresent();
            } catch (final InterruptedException e) {
                // Something bad happened, don't assume leadership. Release the lock being acquired in case it was
                // granted before the interrupt, and the held lock too: an interrupt here usually means shutdown.
                // The interrupt flag is restored before returning, after these releases, since DynamoDB calls made
                // with the flag set could be aborted.
                log.error("Acquiring lock was interrupted in between", e);
                releaseLock(lockClient, targetLeaderLockKey);
                if (switchingLocks) {
                    releaseLock(lockClient, previouslyHeldLockKey);
                }
                interrupted = true;
                response = false;
            } catch (final LockCurrentlyUnavailableException e) {
                response = false;
            }

        } else {
            response = lockItem.get().getOwnerName().equals(workerId);
        }

        String heldKey = response ? targetLeaderLockKey : null;
        if (switchingLocks && !interrupted) {
            if (response) {
                log.info("Acquired {}; releasing {}.", targetLeaderLockKey, previouslyHeldLockKey);
                releaseLock(lockClient, previouslyHeldLockKey);
            } else if (holdsLock(lockClient, previouslyHeldLockKey)) {
                log.info(
                        "Could not acquire {}; keeping {} and trying again at the next key re-evaluation.",
                        targetLeaderLockKey,
                        previouslyHeldLockKey);
                heldKey = previouslyHeldLockKey;
                response = true;
            }
        }

        try {
            // Only the holder of the Leader lock drives table migration; the DeployingLeader only rebalances leases.
            tableMigrationStateMachine.handleLeaderLockResult(response && LeaderLock.LEADER_HASH_KEY.equals(heldKey));
        } catch (final DependencyException | InvalidStateException e) {
            log.warn("handleLeaderLockResult failed, releasing lock and returning false", e);
            if (heldKey != null) {
                releaseLock(lockClient, heldKey);
            }
            heldKey = null;
            response = false;
        }

        recordResult(heldKey, response, segmentingEnabled);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return response;
    }

    /** Whether the lock client holds an unexpired lock for {@code lockKey} owned by this worker. No DynamoDB call. */
    private boolean holdsLock(final AmazonDynamoDBLockClient lockClient, final String lockKey) {
        final Optional<LockItem> lockItem = lockClient.getLock(lockKey, Optional.empty());
        return lockItem.isPresent()
                && !lockItem.get().isExpired()
                && lockItem.get().getOwnerName().equals(workerId);
    }

    private void recordResult(final String heldKey, final boolean response, final boolean segmentingEnabled) {
        heldLeaderLockKey = heldKey;
        segmentingHandler.setHeldLeaderLockKey(heldKey, segmentingEnabled);
        lastCheckTimeInMillis = System.currentTimeMillis();
        lastIsLeaderResult = response;
        publishIsLeaderMetrics(response);
    }

    private Map<String, AttributeValue> getLockAttributes() {
        Map<String, AttributeValue> map = new HashMap<>();
        map.putAll(new LeaderLock().serialize());

        return map;
    }

    private void publishIsLeaderMetrics(final boolean response) {
        final MetricsScope metricsScope =
                MetricsUtil.createMetricsWithOperation(metricsFactory, METRIC_OPERATION_LEADER_DECIDER);
        metricsScope.addData(
                METRIC_OPERATION_LEADER_DECIDER_IS_LEADER, response ? 1 : 0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        MetricsUtil.endScope(metricsScope);
    }

    /**
     * Shuts down the leader decider, releasing any held lock and closing all lock clients.
     * <p>
     * This method releases the leadership lock if held and then closes the underlying
     * DynamoDB lock clients (both legacy and lease table) via the DAO to stop their
     * background heartbeat threads. This ensures that no locks are kept alive after shutdown.
     */
    @Override
    public synchronized void shutdown() {
        if (!isShutdown.getAndSet(true)) {
            releaseLeadershipIfHeld();

            // Close all lock clients to stop background heartbeat threads.
            log.info("Shutting down DynamoDB lock clients for worker {}", workerId);
            coordinatorStateDAO.shutdownLockClients();
        }
    }

    @Override
    public synchronized void releaseLeadershipIfHeld() {
        releaseLeadershipIfHeld(coordinatorStateDAO.getDDBLockClient());
    }

    /**
     * Release the leader lock this worker holds, if any, on the specified lock client. Only the key recorded when the
     * lock was acquired is released. Asking the segmenting handler which key to release could name the other lock,
     * which left the held lock in place until it expired, and it cost a coordinator-table read.
     *
     * @param lockClient the lock client to check and release on
     */
    private void releaseLeadershipIfHeld(final AmazonDynamoDBLockClient lockClient) {
        final String keyToRelease = heldLeaderLockKey;
        if (keyToRelease == null) {
            return;
        }
        try {
            releaseLock(lockClient, keyToRelease);
        } finally {
            heldLeaderLockKey = null;
            // No key, so enablement doesn't matter: the version DAO is told no lock is held.
            segmentingHandler.setHeldLeaderLockKey(null, false);
        }
    }

    /**
     * Releases {@code lockKey} if this worker holds it, using the lock client's local state, so this needs no
     * DynamoDB read. Doesn't change which key is recorded as held.
     */
    private void releaseLock(final AmazonDynamoDBLockClient lockClient, final String lockKey) {
        try {
            // Held locks are in the lock client's local map, so this getLock does no I/O.
            final Optional<LockItem> lockItem = lockClient.getLock(lockKey, Optional.empty());
            if (lockItem.isPresent()
                    && !lockItem.get().isExpired()
                    && lockItem.get().getOwnerName().equals(workerId)) {
                log.info(
                        "Current worker : {} holds the {} lock, releasing it.",
                        lockItem.get().getOwnerName(),
                        lockKey);
                // LockItem.close() releases the lock if this worker owns it. The version is kept in a separate item,
                // so the lock item doesn't need to outlive the release.
                lockItem.get().close();
            }
        } catch (final Exception e) {
            log.error("Failed to release leader lock {}.", lockKey, e);
        }
    }
}
