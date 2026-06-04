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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLockBasedLeaderDecider implements LeaderDecider {

    private final CoordinatorStateDAO coordinatorStateDao;
    private AmazonDynamoDBLockClient dynamoDBLockClient; // during table migration, need to alternate clients/tables
    private final Long leaseDurationMillis;
    private final Long heartbeatPeriodMillis;
    private final String workerId;
    private final MetricsFactory metricsFactory;

    private long lastCheckTimeInMillis = 0L;
    private boolean lastIsLeaderResult = false;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    private long steadySinceEpoch = Instant.now().getEpochSecond();
    private TableMigrationMachine.States tableMigrationStatus = TableMigrationMachine.States.INIT;

    // while table migration is PENDING, need to grab both locks from respective tables (boolean=usingLeaseTable)
    private Map<Boolean, AmazonDynamoDBLockClient> lockClientMap = new HashMap<>();
    private boolean[] lockAcquisitionOrder = getLockAcquisitionOrder();

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
        return new DynamoDBLockBasedLeaderDecider(
                coordinatorStateDao, workerId, leaseDuration, heartbeatPeriod, metricsFactory);
    }

    public static DynamoDBLockBasedLeaderDecider create(
            final CoordinatorStateDAO coordinatorStateDao,
            final String workerId,
            final MetricsFactory metricsFactory,
            long leaseDurationInMillis,
            long heartbeatPeriodInMillis) {
        return create(coordinatorStateDao, workerId, leaseDurationInMillis, heartbeatPeriodInMillis, metricsFactory);
    }

    public DynamoDBLockBasedLeaderDecider(
            final CoordinatorStateDAO coordinatorStateDao,
            final String workerId,
            final Long leaseDuration,
            final Long heartbeatPeriod,
            final MetricsFactory metricsFactory) {
        this.coordinatorStateDao = coordinatorStateDao;
        this.workerId = workerId;
        this.leaseDurationMillis = leaseDuration;
        this.heartbeatPeriodMillis = heartbeatPeriod;
        this.metricsFactory = metricsFactory;
        this.dynamoDBLockClient = createDDBLockClient(coordinatorStateDao.isUsingLeaseTable());
    }

    private AmazonDynamoDBLockClient createDDBLockClient(boolean usingLeaseTable) {
        AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(coordinatorStateDao
                .getDDBLockClientOptionsBuilder(usingLeaseTable)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withLeaseDuration(leaseDurationMillis)
                .withHeartbeatPeriod(heartbeatPeriodMillis)
                .withCreateHeartbeatBackgroundThread(true)
                .withOwnerName(workerId)
                .build());
        // add constructed lock client to boolean map before returning it
        lockClientMap.put(usingLeaseTable, client);
        return client;
    }

    private synchronized void setLockAcquisitionOrder() {
        lockAcquisitionOrder = getLockAcquisitionOrder();
    }

    private boolean[] getLockAcquisitionOrder() {
        String status = tableMigrationStatus.getName();
        switch (status) {
            default: {
                log.warn("Unrecognized table migration status name: " + status + ". Using default lock order.");
            }
            case "INIT":
            case "DEPLOYED": {
                return new boolean[] {false};
            }
            case "PENDING": {
                return new boolean[] {true, false};
            }
            case "COMPLETE": {
                return new boolean[] {true};
            }
        }
    }

    private synchronized void forEachLockClient(Runnable runnable, int maxAttemptsEach) {
        forEachLockClient(() -> {
            // try up to max attempts and count failure if runnable throws exception
            int retries = maxAttemptsEach - 1;
            while (true) {
                try {
                    runnable.run();
                    return true;
                } catch (Exception e) {
                    if (retries-- == 0) {
                        log.warn("Exhausted all attempts during lock client operation: " + e);
                        return false;
                    }
                }
            }
        });
    }

    private synchronized boolean forEachLockClient(BooleanSupplier action) {
        return forEachLockClient(action, null);
    }

    private synchronized boolean forEachLockClient(BooleanSupplier action, Runnable undo) {
        boolean failed = false;
        int i = 0;

        // apply actions in order and break if any fails
        while (i < lockAcquisitionOrder.length) {
            setActiveLockClient(lockAcquisitionOrder[i]);

            if (!action.getAsBoolean()) {
                failed = true;
                break;
            }
            i++;
        }

        // if any action failed, revert all successful actions in reverse order if needed
        if (failed && undo != null) {
            while (i > 0) {
                setActiveLockClient(lockAcquisitionOrder[--i]);
                undo.run();
            }
        }

        return !failed;
    }

    private synchronized void setActiveLockClient(boolean usingLeaseTable) {
        dynamoDBLockClient = lockClientMap.computeIfAbsent(usingLeaseTable, this::createDDBLockClient);
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
    // TODO: confirm whether there's a bug where the worker doesn't grab expired lock if it sees its own workerId
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

        AtomicReference<LockItem> lock = new AtomicReference<>();
        boolean response = tryAcquireAllLocks(lock);

        lastCheckTimeInMillis = System.currentTimeMillis();
        lastIsLeaderResult = response;
        publishIsLeaderMetrics(response);

        if (lock.get() != null) {
            // save the additional attributes from read lock item, leader or not
            // most changes are written by the leader, but some new values need to be picked up from DDB
            saveAdditionalAttributes(lock.get().getAdditionalAttributes());
        }

        // if leader, control table migration state transitions
        if (response) {
            updateTableMigrationStatus();
        }

        return response;
    }

    /**
     * Calls client's tryAcquireLock method. Uses AtomicReference to also return the LockItem read or acquired.
     * @return LockItem if present at all, else null
     */
    private synchronized boolean tryAcquireLock(@NonNull final AtomicReference<LockItem> lock) {
        final Optional<LockItem> lockItem = dynamoDBLockClient.getLock(LEADER_HASH_KEY, Optional.empty());
        lockItem.ifPresent(item -> log.info("Worker : {} is the current leader.", item.getOwnerName()));

        if (!lockItem.isPresent() || lockItem.get().isExpired()) {
            try {
                // current worker does not hold the lock, try to acquire one
                final Optional<LockItem> leaderLockItem =
                        dynamoDBLockClient.tryAcquireLock(AcquireLockOptions.builder(LEADER_HASH_KEY)
                                .withRefreshPeriod(heartbeatPeriodMillis)
                                .withTimeUnit(TimeUnit.MILLISECONDS)
                                .withShouldSkipBlockingWait(true)
                                .withAdditionalAttributes(leaderLockAdditionalAttributes())
                                .build());
                // if leaderLockItem optional is empty, that means the lock is not acquired by this worker
                if (leaderLockItem.isPresent()) {
                    log.info("Worker : {} is new leader", leaderLockItem.get().getOwnerName());
                    lock.compareAndSet(null, leaderLockItem.get());
                    return true;
                }
            } catch (final InterruptedException e) {
                // something bad happened, don't assume leadership and also release lock just in case the
                // lock was granted and still interrupt happened.
                releaseLeadershipIfHeld();
                log.error("Acquiring lock was interrupted in between", e);
            } catch (final LockCurrentlyUnavailableException e) {
                // no-op, just fall through; no need to release leadership because this exception means we don't have it
            }
            // all failure cases fall through here; still give caller the read lock item if it's not populated yet
            lockItem.ifPresent(item -> lock.compareAndSet(null, item));
            return false;
        }
        // lock exists and is not expired; don't overwrite first acquired lock if ref passed through multiple calls
        lock.compareAndSet(null, lockItem.get());
        return lockItem.get().getOwnerName().equals(workerId);
    }

    private synchronized boolean tryAcquireAllLocks(@NonNull final AtomicReference<LockItem> lock) {
        // acquire locks in order -> if any fails, release in reverse order; else, return true with first lock item
        return forEachLockClient(() -> tryAcquireLock(lock), this::releaseLeadershipIfHeld);
    }

    /**
     * Saves the values found in the additional attributes of the leader lock item in DynamoDB to instance variables
     * @param attributes - the map of key-value pairs from the leader lock item in DynamoDB
     */
    private synchronized void saveAdditionalAttributes(@NonNull final Map<String, AttributeValue> attributes) {
        // get attributes from map
        Long ss = DynamoUtils.safeGetLong(attributes, STEADY_SINCE_ATTRIBUTE_NAME);
        String tms = DynamoUtils.safeGetString(attributes, TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME);

        // save values to instance variables
        steadySinceEpoch = ss == null ? Instant.now().getEpochSecond() : ss;
        tableMigrationStatus = StringUtils.isEmpty(tms)
                ? TableMigrationMachine.States.INIT
                : TableMigrationMachine.States.valueOf(tms);
    }

    public synchronized boolean updateTableMigrationStatus() {
        TableMigrationMachine.States newStatus = TableMigrationMachine.update(tableMigrationStatus, steadySinceEpoch);

        if (tableMigrationStatus != newStatus) {
            long newSteadySinceEpoch = Instant.now().getEpochSecond();
            boolean success = false;

            try {
                success = updateTableMigrationStatusInDynamo(newStatus, newSteadySinceEpoch);
            } catch (Exception e) {
                log.warn("Caught exception while trying to write new table migration state {} to DynamoDB!", newStatus);
                // success will still be false
            }

            if (!success) {
                // wait until next table migration machine update to try again
                // machine should deterministically produce same result
                log.warn("Failed to write new table migration state {} to DynamoDB!", newStatus);
                return false;
            }

            // only update local fields to same values if DDB update has succeeded
            tableMigrationStatus = newStatus;
            steadySinceEpoch = newSteadySinceEpoch;
        }
        log.info("Updated table migration state in DynamoDB to {}.", newStatus);
        respondToTableMigrationStatus();
        return true;
    }

    private synchronized boolean updateTableMigrationStatusInDynamo(
            TableMigrationMachine.States newTableMigrationStatus, long newSteadySinceEpoch) {
        // make the request with the expectation that the values we based our decisions on are still there
        return coordinatorStateDao.updateTableMigrationStatusWithExpectation(
                tableMigrationStatus, newTableMigrationStatus, steadySinceEpoch, newSteadySinceEpoch);
    }

    private synchronized void respondToTableMigrationStatus() {
        setLockAcquisitionOrder();

        // have coordinator state DAO decide to use lease table and/or track mutations based on status
        coordinatorStateDao.respondToTableMigrationStatus(String.valueOf(tableMigrationStatus));
        // worker metric stats DAO, on the other hand, bases its decision only on state read at startup
    }

    /**
     * Adds all additional key-value pairs to the attributes map, besides the standard lock item fields
     * @return the map of attributes for DynamoDB
     */
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
            forEachLockClient(this::tryShutdown, 3);
        }
    }

    @Override
    public synchronized void releaseLeadershipIfHeld() {
        forEachLockClient(this::tryReleaseLeadershipIfHeld, 3);
    }

    private synchronized void tryShutdown() throws RuntimeException {
        releaseLeadershipIfHeld();

        // close lock client to stop any potential background heartbeat thread
        try {
            log.info("Closing DynamoDB lock client for worker {}", workerId);
            dynamoDBLockClient.close();
        } catch (final IOException e) {
            log.error("Failed to close DynamoDB lock client for worker {}", workerId, e);
            throw new RuntimeException(e);
        }
    }

    private synchronized void tryReleaseLeadershipIfHeld() throws RuntimeException {
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
            throw new RuntimeException(e);
        }
    }
}
