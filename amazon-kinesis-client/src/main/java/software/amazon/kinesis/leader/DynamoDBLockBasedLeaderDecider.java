package software.amazon.kinesis.leader;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.GetLockOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static java.util.Objects.isNull;
import static software.amazon.kinesis.coordinator.CoordinatorState.LEADER_HASH_KEY;

/**
 * Implementation for LeaderDecider to elect leader using lock on dynamo db table. This class uses
 * AmazonDynamoDBLockClient library to perform the leader election.
 */
@RequiredArgsConstructor
@Slf4j
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

    private long lastIsAnyLeaderElectedDDBReadTimeMillis = 0L;
    private boolean lastIsAnyLeaderElectedResult = false;
    /**
     * Key value pair of LockItem to the time when it was first discovered.
     * If a new LockItem fetched from ddb has different recordVersionNumber than the one in-memory,
     * its considered as new LockItem, and the time when it was fetched is stored in memory to identify lockItem
     * expiry. This is used only in the context of isAnyLeaderElected method.
     */
    private AbstractMap.SimpleEntry<LockItem, Long> lastIsAnyLeaderCheckLockItemToFirstEncounterTime = null;

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

    /**
     * Returns if any ACTIVE leader exists that is elected by the current implementation which can be outside the
     * scope of this worker. That is leader elected by this implementation in any worker in fleet.
     * DynamoDBLockClient does not provide an interface which can tell if an active lock exists or not, thus
     * we need to put custom implementation.
     * The implementation performs DDB get every heartbeatPeriodMillis to have low RCU consumption, which means that
     * the leader could have been elected from the last time the check happened and before check happens again.
     * The information returned from this method has eventual consistency (up to heartbeatPeriodMillis interval).
     *
     * @return true, if any leader is elected else false.
     */
    @Override
    public synchronized boolean isAnyLeaderElected() {
        // Avoid going to ddb for every call and do it once every heartbeatPeriod to have low RCU usage.
        if (Duration.between(
                                Instant.ofEpochMilli(lastIsAnyLeaderElectedDDBReadTimeMillis),
                                Instant.ofEpochMilli(System.currentTimeMillis()))
                        .toMillis()
                > heartbeatPeriodMillis) {
            final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(
                    metricsFactory, this.getClass().getSimpleName() + ":isAnyLeaderElected");
            final long startTime = System.currentTimeMillis();
            try {
                lastIsAnyLeaderElectedDDBReadTimeMillis = System.currentTimeMillis();
                final Optional<LockItem> lockItem = dynamoDBLockClient.getLockFromDynamoDB(
                        GetLockOptions.builder(LEADER_HASH_KEY).build());

                if (!lockItem.isPresent()) {
                    // There is no LockItem in the ddb table, that means no one is holding lock.
                    lastIsAnyLeaderElectedResult = false;
                    log.info("LockItem present : {}", false);
                } else {
                    final LockItem ddbLockItem = lockItem.get();
                    if (isNull(lastIsAnyLeaderCheckLockItemToFirstEncounterTime)
                            || !ddbLockItem
                                    .getRecordVersionNumber()
                                    .equals(lastIsAnyLeaderCheckLockItemToFirstEncounterTime
                                            .getKey()
                                            .getRecordVersionNumber())) {
                        // This is the first isAnyLeaderElected call, so we can't evaluate if the LockItem has expired
                        // or not yet so consider LOCK as ACTIVE.
                        // OR LockItem in ddb and in-memory LockItem have different RecordVersionNumber
                        // and thus the LOCK is still ACTIVE
                        lastIsAnyLeaderElectedResult = true;
                        lastIsAnyLeaderCheckLockItemToFirstEncounterTime =
                                new AbstractMap.SimpleEntry<>(ddbLockItem, lastIsAnyLeaderElectedDDBReadTimeMillis);
                        log.info(
                                "LockItem present : {}, and this is either first call OR lockItem has had "
                                        + "a heartbeat",
                                true);
                    } else {
                        // There is no change in the ddb lock item, so if the last update time is more than
                        // lease duration, the lock is expired else it is still ACTIVE,
                        lastIsAnyLeaderElectedResult = lastIsAnyLeaderCheckLockItemToFirstEncounterTime.getValue()
                                        + ddbLockItem.getLeaseDuration()
                                > lastIsAnyLeaderElectedDDBReadTimeMillis;
                        log.info("LockItem present : {}, and lease expiry: {}", true, lastIsAnyLeaderElectedResult);
                    }
                }
            } catch (final ResourceNotFoundException exception) {
                log.info("Lock table does not exists...");
                // If the table itself doesn't exist, there is no elected leader.
                lastIsAnyLeaderElectedResult = false;
            } finally {
                metricsScope.addData(
                        "Latency",
                        System.currentTimeMillis() - startTime,
                        StandardUnit.MILLISECONDS,
                        MetricsLevel.DETAILED);
                MetricsUtil.endScope(metricsScope);
            }
        }
        return lastIsAnyLeaderElectedResult;
    }
}
