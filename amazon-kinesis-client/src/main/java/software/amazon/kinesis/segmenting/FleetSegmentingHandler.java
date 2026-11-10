package software.amazon.kinesis.segmenting;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leader.LeaderLock;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

/**
 * Component responsible for handling the rolling deployment system. If enabled, workers that are getting deployed with
 * a new lease assignment algorithm will contend for the DeployingLeader lock. Workers that have the new algorithm will
 * have its leases balanced by the DeployingLeader. Leases owned by workers on the deploying version cannot be
 * transferred over unless the lease becomes unassigned.
 *
 * <p>Each leader's version is kept in its own item, managed by {@link LeaderVersionDAO}, not on the leader lock.
 * This handler reads the current Leader's version item to decide which lock a worker contends for.</p>
 */
@Slf4j
@KinesisClientInternalApi
public class FleetSegmentingHandler {

    /** Worker metrics property holding the worker's version hash. */
    public static final String VERSION_HASH_KEY = "versionHash";
    /** Worker metrics property holding when the worker last reported its version hash, in epoch seconds. */
    private static final String VERSION_HASH_LUT_KEY = "versionHashLut";

    @Getter
    private final String versionHash;

    @Getter
    private volatile boolean isVersionEmittedByAllActiveWorkers = false;

    /** Value of {@code enableRollingDeploymentSystem}. */
    private final boolean configEnabled;

    /**
     * Whether the application is in a state where segmenting may run. Segmenting is only valid for the KCL 3.x lease
     * assignment manager after single-table migration has finished, so the Scheduler supplies "client version is 3X
     * and table migration is COMPLETE". Evaluated on every call because both change while the worker runs.
     */
    private final BooleanSupplier segmentingAllowed;

    /**
     * Most recent key this worker held. Not cleared when the lock is lost, so a worker that was the DeployingLeader
     * keeps reporting "not current" until it actually holds the Leader lock.
     */
    private volatile String lastHeldLeaderKey = null;

    /** Reads and heartbeats the leader version items. */
    @Getter
    private final LeaderVersionDAO leaderVersionDAO;

    private final long versionHashExpiryMillis;

    /**
     * Last key computed from a successful read of the Leader's version item. When a read fails, the handler
     * returns these instead of guessing (fail closed), so a transient read error can't change which lock this worker
     * contends for or which leader responsibilities it runs. Null until the first successful read.
     */
    private volatile String lastKnownHashKeyForLeaderLock = null;

    public FleetSegmentingHandler(
            final LeaseManagementConfig config,
            final CoordinatorStateDAO coordinatorStateDAO,
            final ScheduledExecutorService versionHeartbeatExecutor) {
        this(config, coordinatorStateDAO, versionHeartbeatExecutor, () -> true);
    }

    /**
     * @param versionHeartbeatExecutor runs the leader version heartbeat. Owned by the {@link LeaderVersionDAO} and
     *     stopped by {@link #shutdown()}
     */
    public FleetSegmentingHandler(
            final LeaseManagementConfig config,
            final CoordinatorStateDAO coordinatorStateDAO,
            final ScheduledExecutorService versionHeartbeatExecutor,
            final BooleanSupplier segmentingAllowed) {
        this.segmentingAllowed = segmentingAllowed;
        this.versionHash = String.valueOf(config.leaseAssignmentStrategy().getVersionHash());
        this.configEnabled = config.enableRollingDeploymentSystem();
        this.versionHashExpiryMillis = expiryMillis(config);
        // The heartbeat is scheduled whenever the flag is on; each tick checks isEnabled(), since segmenting may
        // only become allowed after construction (client version 3X, table migration COMPLETE).
        this.leaderVersionDAO = new LeaderVersionDAO(
                coordinatorStateDAO,
                versionHash,
                config.workerIdentifier(),
                versionHeartbeatExecutor,
                config.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis(),
                configEnabled,
                this::isEnabled);
    }

    @VisibleForTesting
    FleetSegmentingHandler(
            final LeaseManagementConfig config,
            final LeaderVersionDAO leaderVersionDAO,
            final BooleanSupplier segmentingAllowed) {
        this.segmentingAllowed = segmentingAllowed;
        this.versionHash = String.valueOf(config.leaseAssignmentStrategy().getVersionHash());
        this.configEnabled = config.enableRollingDeploymentSystem();
        this.versionHashExpiryMillis = expiryMillis(config);
        this.leaderVersionDAO = leaderVersionDAO;
    }

    /**
     * The expiry threshold is the max of two leader heartbeats or two cycles of workers emitting metrics, which is
     * the current expiry threshold for workers.
     */
    private static long expiryMillis(final LeaseManagementConfig config) {
        return Math.max(
                2 * config.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis(),
                2 * config.workerUtilizationAwareAssignmentConfig().workerMetricsReporterFreqInMillis());
    }

    /**
     * Whether segmenting is active right now: the config flag is on, and the application is on client version 3X with
     * table migration COMPLETE. When this is false the worker behaves exactly as it did before segmenting.
     */
    public boolean isEnabled() {
        if (!configEnabled) {
            return false;
        }
        try {
            return segmentingAllowed.getAsBoolean();
        } catch (final RuntimeException e) {
            // e.g. the migration state machine isn't initialized yet; segmenting stays off until it is.
            log.debug("Segmenting not allowed yet: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Returns the key of the leader lock to take depending on whether the worker is part of the deploying version.
     *
     * <p>If the Leader's version item can't be read, this returns the key from the last successful read rather than
     * defaulting to "Leader". Defaulting would let a single transient read error make a deploying-version worker take
     * the Leader lock and write its own version to the Leader's version item, flipping the fleet's algorithm.</p>
     *
     * @return Key of the leader lock (either "Leader" or "DeployingLeader").
     * @throws VersionStateUnavailableException if the read fails and no previous read has succeeded.
     */
    public String getHashKeyForLeaderLock() {
        // If segmenting handler is disabled, default to returning the Leader key
        if (!isEnabled()) {
            log.debug("Segmenting handler is disabled, using {} as leader lock.", LeaderLock.LEADER_HASH_KEY);
            return LeaderLock.LEADER_HASH_KEY;
        }
        final LeaderVersionState currentVersion;
        try {
            currentVersion = leaderVersionDAO.getVersionState(LeaderLock.LEADER_HASH_KEY);
        } catch (final Exception e) {
            // if any transient failures occur when trying to use DDB, fallback to using the last known value
            final String lastKnownKey = lastKnownHashKeyForLeaderLock;
            if (lastKnownKey == null) {
                // if there has never been a successful call, throw an Exception
                throw new VersionStateUnavailableException(
                        "Failed to read " + LeaderVersionState.LEADER_VERSION_HASH_KEY
                                + " and no previous value is known; not choosing a leader lock.",
                        e);
            }
            log.warn(
                    "Failed to read {}; keeping last known leader lock key {}.",
                    LeaderVersionState.LEADER_VERSION_HASH_KEY,
                    lastKnownKey,
                    e);
            return lastKnownKey;
        }
        final String key = computeHashKeyForLeaderLock(currentVersion);
        lastKnownHashKeyForLeaderLock = key;
        return key;
    }

    private String computeHashKeyForLeaderLock(final LeaderVersionState currentVersion) {
        // If the current leader's version item does not exist, default to obtaining the Leader lock.
        // If it does exist but is expired, then the versioned workers were rolled back (or the current leader is
        // gone). For both cases, obtain the normal leader lock.
        if (currentVersion == null
                || currentVersion.getVersionHash() == null
                || currentVersion.isExpired(System.currentTimeMillis(), versionHashExpiryMillis)) {
            if (currentVersion != null) {
                log.debug(
                        "Returning {} (not on current version). currentVersion={}, myVersionHash={}, expiryMs={}",
                        LeaderLock.LEADER_HASH_KEY,
                        currentVersion,
                        versionHash,
                        versionHashExpiryMillis);
            } else {
                log.info(
                        "Returning {} because {} doesn't exist.",
                        LeaderLock.LEADER_HASH_KEY,
                        LeaderVersionState.LEADER_VERSION_HASH_KEY);
            }
            return LeaderLock.LEADER_HASH_KEY;
        }

        // If all workers are emitting the same version, take the Leader lock.
        // If the current version does exist and is not expired, take the leader lock depending on the version hash.
        final boolean versionHashMatches = versionHash.equals(currentVersion.getVersionHash());
        if (isVersionEmittedByAllActiveWorkers || versionHashMatches) {
            log.debug(
                    "isVersionEmittedByAllActiveWorkers : {}, doesVersionHashMatch : {}",
                    isVersionEmittedByAllActiveWorkers,
                    versionHashMatches);
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

    /**
     * Whether this leader should handle the current Leader responsibilities (expired/unassigned lease assignment,
     * shard sync) as opposed to the DeployingLeader responsibilities. If the segmenting handler is disabled, the
     * leader behaves as it normally would without segmenting.
     *
     * <p>Decided by which lock this worker holds, not by re-reading the Leader item: only the holder of the Leader
     * lock is current. This needs no DynamoDB read, so a throttled coordinator table can't make the real current
     * leader skip its duties, and a DeployingLeader holder is never "current" just because the Leader item's version
     * hash happens to match its own (the end-of-rollforward double-current case). A worker that has never held a
     * leader lock, e.g. when a non-lock leader decider is active, is treated as current.</p>
     */
    public boolean isOnCurrentVersion() {
        if (!isEnabled()) {
            return true;
        }
        return !LeaderLock.DEPLOYING_LEADER_HASH_KEY.equals(lastHeldLeaderKey);
    }

    /**
     * Stops the version heartbeat. Called by the Scheduler during shutdown, before the leader lock is released, so a
     * shut-down worker can't keep refreshing its version item. Safe to call more than once.
     */
    public void shutdown() {
        leaderVersionDAO.shutdown();
    }

    /**
     * Records which leader lock this worker holds after a leadership check. Called by the leader decider. Also tells
     * the {@link LeaderVersionDAO}, which writes and heartbeats the held lock's version item.
     *
     * @param key the held lock key, or null if this worker holds neither lock
     */
    public void setHeldLeaderKey(final String key) {
        leaderVersionDAO.setHeldLockKey(isEnabled() ? key : null);
        if (key != null) {
            lastHeldLeaderKey = key;
        } else {
            // The flag is only computed by a leader's assignment run. Once this worker holds no lock it is never
            // refreshed, so clear it rather than keep steering this worker to the Leader key on an old observation.
            isVersionEmittedByAllActiveWorkers = false;
        }
    }

    public boolean isWorkerVersionHashStale(final WorkerMetricStats worker) {
        if (worker.getProperties() == null) {
            return true;
        }
        final String lutStr = worker.getProperties().get(VERSION_HASH_LUT_KEY);
        if (lutStr == null) {
            return true;
        }
        try {
            return isVersionHashExpired(Long.parseLong(lutStr));
        } catch (final NumberFormatException e) {
            log.warn(
                    "Unparseable {} '{}' on worker {}; treating as stale.",
                    VERSION_HASH_LUT_KEY,
                    lutStr,
                    worker.getWorkerId());
            return true;
        }
    }

    /**
     * Records whether every active worker reports this worker's version hash. An empty active fleet (e.g. every
     * WorkerMetricStats entry transiently expired) says nothing about the rollout, so it counts as "not all".
     */
    public void setIsVersionEmittedByAllActiveWorkers(
            final List<WorkerMetricStats> activeWorkerMetrics, final List<WorkerMetricStats> workersOnVersionHash) {
        isVersionEmittedByAllActiveWorkers = !activeWorkerMetrics.isEmpty()
                && toWorkerIds(activeWorkerMetrics).equals(toWorkerIds(workersOnVersionHash));
    }

    private static Set<String> toWorkerIds(final List<WorkerMetricStats> workers) {
        return workers.stream().map(WorkerMetricStats::getWorkerId).collect(Collectors.toSet());
    }

    public List<WorkerMetricStats> filterWorkersOnVersionHash(final List<WorkerMetricStats> activeWorkers) {
        return activeWorkers.stream()
                // versionHash.equals(...) rather than the reverse: a properties map without versionHash (the field
                // predates segmenting) must not throw inside the assignment manager.
                .filter(workerMetricStats -> workerMetricStats.getProperties() != null
                        && versionHash.equals(workerMetricStats.getProperties().get(VERSION_HASH_KEY))
                        && !isWorkerVersionHashStale(workerMetricStats))
                .collect(Collectors.toList());
    }

    public List<WorkerMetricStats> filterWorkersWithNoOrStaleVersionHash(final List<WorkerMetricStats> activeWorkers) {
        return activeWorkers.stream()
                .filter(worker -> !doesWorkerHaveVersionHash(worker) || isWorkerVersionHashStale(worker))
                .collect(Collectors.toList());
    }

    private boolean isVersionHashExpired(final long lut) {
        return Duration.between(Instant.ofEpochSecond(lut), Instant.now()).toMillis() > versionHashExpiryMillis;
    }

    private boolean doesWorkerHaveVersionHash(final WorkerMetricStats worker) {
        return worker.getProperties() != null && worker.getProperties().containsKey(VERSION_HASH_KEY);
    }
}
