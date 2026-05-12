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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.coordinator.migration.TableMigrationMachine;
import software.amazon.kinesis.leases.DynamoUtils;
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

    private final CoordinatorStateDAO coordinatorStateDao;
    private final AmazonDynamoDBLockClient dynamoDBLockClient;
    private final Long leaseDurationMillis;
    private final Long heartbeatPeriodMillis;
    private final String workerId;
    private final MetricsFactory metricsFactory;

    private long lastCheckTimeInMillis = 0L;
    private boolean lastIsLeaderResult = false;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    public volatile long steadySinceEpoch = Instant.now().getEpochSecond();
    public volatile TableMigrationMachine.States tableMigrationStatus = TableMigrationMachine.States.DEPLOYING;

    @Getter
    private PriorityQueue<ScheduledUpdate> scheduledUpdatePriorityQueue = new PriorityQueue<>();

    /** this lock client is for acquiring the leader lock item in the other table (lease, coordinator states) */
    private AmazonDynamoDBLockClient otherDynamoDBLockClient = null;

    public static final String ENTITY_TYPE_ATTRIBUTE_NAME = "entityType";
    public static final String LEADER_LOCK_ENTITY_TYPE = "leaderLock";
    public static final String STEADY_SINCE_ATTRIBUTE_NAME = "steadySince";
    public static final String TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME =
            TableMigrationMachine.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME;

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
                coordinatorStateDao, dynamoDBLockClient, leaseDuration, heartbeatPeriod, workerId, metricsFactory);
    }

    public static DynamoDBLockBasedLeaderDecider create(
            final CoordinatorStateDAO coordinatorStateDao,
            final String workerId,
            final MetricsFactory metricsFactory,
            long leaseDurationInMillis,
            long heartbeatPeriodInMillis) {
        return create(coordinatorStateDao, workerId, leaseDurationInMillis, heartbeatPeriodInMillis, metricsFactory);
    }

    private AmazonDynamoDBLockClient createDDBLockClient(boolean usingLeaseTable) {
        return new AmazonDynamoDBLockClient(coordinatorStateDao
                .getDDBLockClientOptionsBuilder(usingLeaseTable)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withLeaseDuration(leaseDurationMillis)
                .withHeartbeatPeriod(heartbeatPeriodMillis)
                .withCreateHeartbeatBackgroundThread(true)
                .withOwnerName(workerId)
                .build());
    }

    @Override
    public void initialize() {
        log.info("Initializing DDB Lock based leader decider");
        saveAdditionalAttributes(coordinatorStateDao.getLeaderLockItemSnapshot());
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

        // try acquiring leader lock; if succeeds, must also grab other lock if table migration status is not COMPLETE
        LockItem lock = tryAcquireLock(dynamoDBLockClient, LEADER_HASH_KEY, leaderLockAdditionalAttributes());
        boolean response = lock != null && lock.getOwnerName().equals(workerId) && tryAcquireOtherLockIfNeeded(lock);

        lastCheckTimeInMillis = System.currentTimeMillis();
        lastIsLeaderResult = response;
        publishIsLeaderMetrics(response);
        // if not leader, update queue will be empty; blocks caller of isLeader() unless all updates run async
        processScheduledUpdateQueue(response);
        if (response) {
            // if leader, lock item will be present because we just acquired it; save the additional attributes from it
            saveAdditionalAttributes(lock.getAdditionalAttributes());
        }
        return response;
    }

    /** TODO: fix merge conflicts with Vincent's PR for fleet segmenting handler */
    private synchronized LockItem tryAcquireLock(
            AmazonDynamoDBLockClient client, String key, Map<String, AttributeValue> additionalAttributes) {
        final Optional<LockItem> lockItem = client.getLock(key, Optional.empty());
        lockItem.ifPresent(item -> log.info("Worker : {} is the current leader.", item.getOwnerName()));

        if (!lockItem.isPresent() || lockItem.get().isExpired()) {
            try {
                // current worker does not hold the lock, try to acquire one
                final Optional<LockItem> leaderLockItem = client.tryAcquireLock(AcquireLockOptions.builder(key)
                        .withRefreshPeriod(heartbeatPeriodMillis)
                        .withTimeUnit(TimeUnit.MILLISECONDS)
                        .withShouldSkipBlockingWait(true)
                        .withAdditionalAttributes(additionalAttributes)
                        .build());
                // if leaderLockItem optional is empty, that means the lock is not acquired by this worker
                if (leaderLockItem.isPresent()) {
                    log.info("Worker : {} is new leader", leaderLockItem.get().getOwnerName());
                    return leaderLockItem.get();
                }
                return null;
            } catch (final InterruptedException e) {
                // something bad happened, don't assume leadership and also release lock just in case the
                // lock was granted and still interrupt happened.
                releaseLeadershipIfHeld();
                log.error("Acquiring lock was interrupted in between", e);
                return null;

            } catch (final LockCurrentlyUnavailableException e) {
                return null;
            }
        }
        return lockItem.get();
    }

    private synchronized boolean tryAcquireOtherLockIfNeeded(LockItem lock) {
        for (int i = 0; i < 100; i++) {
            log.info("Trying to find out if I need to grab the other lock and will try if needed!");
        }

        // check if table migration status is COMPLETE (will be in additional attributes if populated)
        String tableMigrationStatus = coordinatorStateDao.getTableMigrationStatus(lock.getAdditionalAttributes());
        if (tableMigrationStatus == null) {
            for (int i = 0; i < 100; i++) {
                log.info("Table migration status not found! We don't have to grab the other lock!");
            }
            // no table migration status (e.g. table migration never requested via config) -> no other lock to grab
            return true;
        }
        if (TableMigrationMachine.compareStatuses(tableMigrationStatus, "COMPLETE") >= 0) {
            // while table migration is incomplete, leader must also grab lock from other table to be leader
            if (otherDynamoDBLockClient == null) {
                // create lock client for other table than the one coordinatorStateDao is using
                otherDynamoDBLockClient = createDDBLockClient(!coordinatorStateDao.isUsingLeaseTable());
            }
            LockItem otherLock =
                    tryAcquireLock(otherDynamoDBLockClient, LEADER_HASH_KEY, leaderLockAdditionalAttributes());
            if (otherLock == null || !otherLock.getOwnerName().equals(workerId)) {
                for (int i = 0; i < 100; i++) {
                    log.info("Couldn't grab the other lock! Need to release leadership if held!");
                }
                // couldn't grab other lock, must release the one already grabbed
                releaseLeadershipIfHeld();
                return false;
                // if we grabbed the other lock previously and now release, we won't be able to heartbeat the other lock
                // unless we grab the first one again, so no need to explicitly release the "other" lock
            }
        }
        return true;
    }

    /**
     * Convenience method to call ScheduledUpdate constructor given DynamoDBLockBasedLeaderDecider instance.
     * @param interval - the target frequency of the update, in milliseconds
     * @param update - the update function (consumes DynamoDBLockBasedLeaderDecider)
     */
    public void createScheduledUpdate(long interval, Consumer<DynamoDBLockBasedLeaderDecider> update) {
        new ScheduledUpdate(interval, update);
    }

    /**
     * Processes the scheduled update queue with the option to not reschedule any of the scheduled updates.
     * This allows the leader to finish up upon leader change, without missing the already queued updates.
     * @param shouldReschedule - whether to not cancel all scheduled updates in the queue
     */
    private void processScheduledUpdateQueue(boolean shouldReschedule) {
        if (!shouldReschedule) {
            for (ScheduledUpdate update : scheduledUpdatePriorityQueue) {
                update.setCanceled(true);
            }
        }
        processScheduledUpdateQueue();
    }

    private void processScheduledUpdateQueue() {
        ScheduledUpdate update;
        while ((update = scheduledUpdatePriorityQueue.peek()) != null
                && update.priority <= Instant.now().toEpochMilli()) {
            scheduledUpdatePriorityQueue.poll().execute();
        }
    }

    public class ScheduledUpdate implements Comparable<ScheduledUpdate> {

        long lastRun;
        long priority;
        final long interval;
        final Consumer<DynamoDBLockBasedLeaderDecider> update;
        final DynamoDBLockBasedLeaderDecider leaderDecider = DynamoDBLockBasedLeaderDecider.this;

        @Setter
        boolean canceled;

        ScheduledUpdate(long interval, Consumer<DynamoDBLockBasedLeaderDecider> update) {
            this.interval = interval;
            this.update = update;

            // always execute scheduled update immediately upon instantiation (adds itself to queue when done)
            execute();
        }

        /**
         * Invoke lambda (consumes the outer leaderDecider) and add this to queue with next due instant as priority
         */
        private void execute() {
            try {
                update.accept(leaderDecider);
            } catch (Exception e) {
                log.error("Current or recent leader failed scheduled update: ", e);
            } finally {
                // always (re-)add to queue unless canceled, even if update fails
                lastRun = Instant.now().toEpochMilli();
                priority = lastRun + interval;
                if (!canceled) {
                    leaderDecider.getScheduledUpdatePriorityQueue().add(this);
                }
            }
        }

        @Override
        public int compareTo(ScheduledUpdate other) {
            return Long.compare(priority, other.priority);
        }
    }

    /**
     * Saves the values found in the additional attributes of the leader lock item in DynamoDB to instance variables
     * @param attributes - the map of key-value pairs from the leader lock item in DynamoDB
     */
    private void saveAdditionalAttributes(final Map<String, AttributeValue> attributes) {
        if (attributes == null) {
            // nothing to save; default or latest in-memory values will be written
            return;
        }
        Long ss = null;
        String tms = null;

        // get attributes from map
        if (attributes != null) {
            ss = DynamoUtils.safeGetLong(attributes, STEADY_SINCE_ATTRIBUTE_NAME);
            tms = DynamoUtils.safeGetString(attributes, TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME);
        }

        // save values to instance variables
        steadySinceEpoch = ss == null ? Instant.now().getEpochSecond() : ss;
        tableMigrationStatus =
                tms == null ? TableMigrationMachine.States.DEPLOYING : TableMigrationMachine.States.valueOf(tms);
    }

    /**
     * Adds all additional key-value pairs to the attributes map, besides the standard lock item fields
     * @return the map of attributes for DynamoDB
     */

    // TODO: will cause merge conflict with Vincent's PR where he adds versionHashLut to additionalAttributes
    // TODO: fix by calling fleetSegmentingHandler.getVersionHashLutForLeaderLockTable() here instead of above
    private Map<String, AttributeValue> leaderLockAdditionalAttributes() {
        Map<String, AttributeValue> attributes = new HashMap<>();

        // add entity type additional attribute
        attributes.put(ENTITY_TYPE_ATTRIBUTE_NAME, AttributeValue.fromS(LEADER_LOCK_ENTITY_TYPE));
        // add steady since additional attribute
        attributes.put(STEADY_SINCE_ATTRIBUTE_NAME, AttributeValue.fromN(String.valueOf(steadySinceEpoch)));
        // add table migration status additional attribute
        attributes.put(TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME, AttributeValue.fromS(tableMigrationStatus.getName()));

        return attributes;
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
