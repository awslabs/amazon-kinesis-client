package software.amazon.kinesis.segmenting;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leader.LeaderLock;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

/**
 * Component responsible for handling the rolling deployment system. If enabled, workers that are getting deployed with
 * a new lease assignment algorithm will contend for the DeployingLeader lock. Workers that have the new algorithm will
 * have its leases balanced by the DeployingLeader. Leases owned by workers on the deploying version cannot be
 * transferred over unless the lease becomes unassigned.
 */
@Slf4j
@KinesisClientInternalApi
public class FleetSegmentingHandler {

    public static final String VERSION_HASH_KEY = "versionHash";
    private static final String VERSION_HASH_LUT_KEY = "versionHashLut";
    private static final String LEADER_LOCK_OWNER_KEY = "ownerName";

    @Getter
    private final String versionHash;

    @Getter
    private volatile boolean isVersionEmittedByAllActiveWorkers = false;

    @Getter
    private final boolean isEnabled;

    @Setter
    private volatile boolean isLeader = false;

    private final CoordinatorStateDAO coordinatorStateDAO;
    private final long versionHashExpiryMillis;
    private final String workerId;

    /**
     * Last results computed from a successful read of the Leader lock's version state. When a read fails, the handler
     * returns these instead of guessing (fail closed), so a transient read error can't change which lock this worker
     * contends for or which leader responsibilities it runs. Null until the first successful read.
     */
    private volatile String lastKnownHashKeyForLeaderLock = null;

    private volatile Boolean lastKnownIsOnCurrentVersion = null;

    public FleetSegmentingHandler(
            final LeaseManagementConfig config,
            final CoordinatorStateDAO coordinatorStateDAO,
            final ScheduledExecutorService versionHeartbeatExecutor) {
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.versionHash = String.valueOf(config.leaseAssignmentStrategy().getVersionHash());
        isEnabled = config.enableRollingDeploymentSystem();
        workerId = config.workerIdentifier();

        // The expiry threshold will be the max of two leader heartbeats or two cycles of workers emitting metrics,
        // which is the current expiry threshold for workers.
        this.versionHashExpiryMillis = Math.max(
                2 * config.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis(),
                2 * config.workerUtilizationAwareAssignmentConfig().workerMetricsReporterFreqInMillis());

        if (isEnabled) {
            // start a thread to update the leader's version hash only if segmenting is enabled
            versionHeartbeatExecutor.scheduleAtFixedRate(
                    this::updateLeaderVersionHashLastUpdateTime,
                    0,
                    config.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Returns the key of the leader lock to take depending on whether the worker is part of the deploying version.
     *
     * <p>If the Leader lock's version state can't be read, this returns the key from the last successful read rather
     * than defaulting to "Leader". Defaulting would let a single transient read error make a deploying-version worker
     * take the Leader lock and stamp its own version hash on it, flipping the fleet's algorithm.</p>
     *
     * @return Key of the leader lock (either "Leader" or "DeployingLeader").
     * @throws VersionStateUnavailableException if the read fails and no previous read has succeeded.
     */
    public String getHashKeyForLeaderLock() {
        // If segmenting handler is disabled, default to returning the Leader key
        if (!isEnabled) {
            log.debug("Segmenting handler is disabled, using {} as leader lock.", LeaderLock.LEADER_HASH_KEY);
            return LeaderLock.LEADER_HASH_KEY;
        }
        final Map<String, AttributeValue> currentVersionAttrs;
        try {
            currentVersionAttrs = getCoordinatorStateAttributes(LeaderLock.LEADER_HASH_KEY);
        } catch (final Exception e) {
            // if any transient failures occur when trying to use DDB, fallback to using the last known value
            final String lastKnownKey = lastKnownHashKeyForLeaderLock;
            if (lastKnownKey == null) {
                // if there has never been a successful call, throw an Exception
                throw new VersionStateUnavailableException(
                        "Failed to read " + LeaderLock.LEADER_HASH_KEY
                                + " version state and no previous value is known; not choosing a leader lock.",
                        e);
            }
            log.warn(
                    "Failed to read {} version state; keeping last known leader lock key {}.",
                    LeaderLock.LEADER_HASH_KEY,
                    lastKnownKey,
                    e);
            return lastKnownKey;
        }
        final String key = computeHashKeyForLeaderLock(currentVersionAttrs);
        lastKnownHashKeyForLeaderLock = key;
        return key;
    }

    private String computeHashKeyForLeaderLock(final Map<String, AttributeValue> currentVersionAttrs) {
        // If the current leader does not exist, default to obtaining the Leader lock.
        // If the current leader does exist but is expired, then the versioned workers were rolled back. For both
        // cases, obtain the normal leader lock.
        if (currentVersionAttrs == null
                || !currentVersionAttrs.containsKey(VERSION_HASH_KEY)
                || isVersionHashExpired(currentVersionAttrs)) {
            if (currentVersionAttrs != null) {
                log.debug(
                        "Returning {} (not on current version). attrs={}, myVersionHash={}, expiryMs={}, isLeader={}",
                        LeaderLock.LEADER_HASH_KEY,
                        currentVersionAttrs,
                        versionHash,
                        versionHashExpiryMillis,
                        isLeader);
            } else {
                log.info("Returning {} due to null version attributes.", LeaderLock.LEADER_HASH_KEY);
            }
            return LeaderLock.LEADER_HASH_KEY;
        }

        // If all workers are emitting the same version, take the Leader lock.
        // If the current version does exist and the version hash is not expired, take the leader lock depending on
        // the value of the version hash.
        if (isVersionEmittedByAllActiveWorkers || doesVersionHashMatch(currentVersionAttrs)) {
            log.debug(
                    "isVersionEmittedByAllActiveWorkers : {}, doesVersionHashMatch : {}",
                    isVersionEmittedByAllActiveWorkers,
                    doesVersionHashMatch(currentVersionAttrs));
            return LeaderLock.LEADER_HASH_KEY;
        }
        return LeaderLock.DEPLOYING_LEADER_HASH_KEY;
    }

    public Map<String, String> generateVersionHashWithLastUpdatedTimeMap() {
        final Map<String, String> workerProperties = new HashMap<>();
        workerProperties.put(VERSION_HASH_KEY, versionHash);
        workerProperties.put(VERSION_HASH_LUT_KEY, String.valueOf(Instant.now().getEpochSecond()));
        return workerProperties;
    }

    public Map<String, AttributeValue> generateVersionHashWithLastUpdatedTimeForLockTable() {
        Map<String, AttributeValue> workerProperties = new HashMap<>();
        generateVersionHashWithLastUpdatedTimeMap().forEach((k, v) -> workerProperties.put(k, AttributeValue.fromS(v)));
        return workerProperties;
    }

    /**
     * The "current" version is the version that is currently deployed to all workers. Used by the leader to check
     * if it should handle the current Leader responsibilities (as opposed to the DeployingLeader responsibilities).
     * If the segmenting handler is disabled, the leader will behave as it normally would without segmenting.
     *
     * <p>If the version state can't be read, this returns the result of the last successful read, so a transient
     * error doesn't make the real current leader skip expired-lease assignment and shard sync. With no previous
     * successful read it returns false, so an unverified leader never takes on current-leader-only work.</p>
     */
    public boolean isOnCurrentVersion() {
        if (!isEnabled) {
            return true;
        }
        final Map<String, AttributeValue> attrs;
        try {
            attrs = getCoordinatorStateAttributes(LeaderLock.LEADER_HASH_KEY);
        } catch (final Exception e) {
            final Boolean lastKnown = lastKnownIsOnCurrentVersion;
            log.warn(
                    "Failed to read {} version state; using last known isOnCurrentVersion={}.",
                    LeaderLock.LEADER_HASH_KEY,
                    lastKnown,
                    e);
            return lastKnown != null && lastKnown;
        }
        final boolean result = doesVersionHashMatch(attrs);
        lastKnownIsOnCurrentVersion = result;
        return result;
    }

    public boolean isWorkerVersionHashStale(final WorkerMetricStats worker) {
        if (worker.getProperties() == null) {
            return true;
        }
        String lutStr = worker.getProperties().get(VERSION_HASH_LUT_KEY);
        if (lutStr == null) {
            return true;
        }
        // TODO: add guard for parselong
        return isVersionHashExpired(Long.parseLong(lutStr));
    }

    // TODO: check for empty set
    public void setIsVersionEmittedByAllActiveWorkers(
            final List<WorkerMetricStats> activeWorkerMetrics, final List<WorkerMetricStats> workersOnVersionHash) {
        isVersionEmittedByAllActiveWorkers = toWorkerIds(activeWorkerMetrics).equals(toWorkerIds(workersOnVersionHash));
    }

    private static Set<String> toWorkerIds(final List<WorkerMetricStats> workers) {
        return workers.stream().map(WorkerMetricStats::getWorkerId).collect(Collectors.toSet());
    }

    // TODO: NPE: getProperties().get(VERSION_HASH_KEY).equals(getVersionHash()) (:175-179). Any WorkerMetricStats row
    // whose properties map exists without versionHash (the field predates the PR as an extensibility map) throws inside
    // the LAM; 3 failures release the leader, the next leader reads the same row, and nobody assigns expired leases.
    public List<WorkerMetricStats> filterWorkersOnVersionHash(final List<WorkerMetricStats> activeWorkers) {
        return activeWorkers.stream()
                .filter(workerMetricStats -> workerMetricStats.getProperties() != null
                        && workerMetricStats
                                .getProperties()
                                .get(VERSION_HASH_KEY)
                                .equals(getVersionHash())
                        && !isWorkerVersionHashStale(workerMetricStats))
                .collect(Collectors.toList());
    }

    public List<WorkerMetricStats> filterWorkersWithNoOrStaleVersionHash(final List<WorkerMetricStats> activeWorkers) {
        return activeWorkers.stream()
                .filter(worker -> !doesWorkerHaveVersionHash(worker) || isWorkerVersionHashStale(worker))
                .collect(Collectors.toList());
    }

    private void updateLeaderVersionHashLastUpdateTime() {
        // checking if worker is the leader or if segmenting is enabled to avoid unnecessary calls to DDB
        if (!isLeader || !isEnabled) {
            return;
        }
        try {
            // Inside the try: an exception escaping a scheduleAtFixedRate task cancels all future heartbeats.
            final String leaderKey = getHashKeyForLeaderLock();
            final Map<String, ExpectedAttributeValue> expectations = getVersionHashHeartbeatExpectationMap(leaderKey);
            final CoordinatorState state = CoordinatorState.builder()
                    .key(leaderKey)
                    .attributes(Collections.singletonMap(
                            VERSION_HASH_LUT_KEY,
                            AttributeValue.fromS(String.valueOf(Instant.now().getEpochSecond()))))
                    .build();
            coordinatorStateDAO.updateCoordinatorStateWithExpectation(state, expectations);
        } catch (final Exception e) {
            log.error("Failed to update leader versionHashLut", e);
        }
    }

    private Map<String, ExpectedAttributeValue> getVersionHashHeartbeatExpectationMap(final String leaderKey) {
        final Map<String, ExpectedAttributeValue> expectations = new HashMap<>();
        expectations.put(
                VERSION_HASH_LUT_KEY,
                ExpectedAttributeValue.builder()
                        .comparisonOperator(ComparisonOperator.NOT_NULL)
                        .build());
        expectations.put(
                LEADER_LOCK_OWNER_KEY,
                ExpectedAttributeValue.builder()
                        .value(AttributeValue.fromS(workerId))
                        .build());
        return expectations;
    }

    private boolean isVersionHashExpired(final Map<String, AttributeValue> item) {
        return item.containsKey(VERSION_HASH_KEY)
                && item.containsKey(VERSION_HASH_LUT_KEY)
                && isVersionHashExpired(
                        Long.parseLong(item.get(VERSION_HASH_LUT_KEY).s()));
    }

    private boolean isVersionHashExpired(final long lut) {
        return Duration.between(Instant.ofEpochSecond(lut), Instant.now()).toMillis() > versionHashExpiryMillis;
    }

    private Map<String, AttributeValue> getCoordinatorStateAttributes(final String key)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final CoordinatorState state = coordinatorStateDAO.getCoordinatorState(key);
        return state != null ? state.getAttributes() : null;
    }

    private boolean doesVersionHashMatch(final Map<String, AttributeValue> attrs) {
        return attrs != null
                && attrs.containsKey(VERSION_HASH_KEY)
                && versionHash.equals(attrs.get(VERSION_HASH_KEY).s());
    }

    private boolean doesWorkerHaveVersionHash(final WorkerMetricStats worker) {
        return worker.getProperties() != null && worker.getProperties().containsKey(VERSION_HASH_KEY);
    }
}
