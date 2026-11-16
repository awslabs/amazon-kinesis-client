package software.amazon.kinesis.segmenting;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import software.amazon.kinesis.leases.Lease;
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

    /**
     * How long after its last heartbeat {@code LeaderVersion} still counts as the fleet's current version. Until then,
     * a worker on another version contends for DeployingLeader, even if the Leader holder has stopped heartbeating
     * (e.g. crashed), unless every active worker reports its version. After that, every worker contends for Leader.
     *
     * <p>This is much longer than the Leader lock lease on purpose. When the Leader holder crashes, its lock only
     * becomes acquirable after the lease (2 minutes by default). If the version expired first, workers on the
     * deploying version would race current-version workers for the lock, and the winner's version would become
     * current. Ten minutes lets a current-version worker take the lock, and lets the DeployingLeader see that the old
     * version is gone (its workers' metrics go stale after {@link #workerVersionExpiryMillis}) and move to Leader
     * itself, before the version expires. The limit only matters when neither can happen, e.g. a fleet left on two new
     * versions, or with workers that report no version: it bounds how long the fleet goes without a current leader.</p>
     */
    static final long LEADER_VERSION_EXPIRY_MILLIS = Duration.ofMinutes(10).toMillis();

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

    /** Key of the lock this worker holds right now, or null. Cleared when the lock is released or lost. */
    private volatile String currentlyHeldLeaderKey = null;

    /**
     * What the leader's assignment run saw when it last found every active worker on this worker's version: when, and
     * each lease's owner and counter. Null when the last run didn't find that. Used to re-validate the flip.
     */
    private volatile AllOnVersionSnapshot allOnVersionSnapshot = null;

    /**
     * Reads the worker metrics and leases fresh, to re-validate the flip to the Leader lock at the moment it is
     * decided. Set by the Scheduler once its DAOs exist. Until set, the flip through all-workers-on-version doesn't
     * happen (fail closed); the expiry path still works.
     */
    private volatile FleetViewReader fleetViewReader = null;

    /** Lease duration; a lease whose counter hasn't moved for this long has a dead owner. */
    private final long leaseDurationMillis;

    /** Age after which a worker's metrics row means the worker is gone. Same rule as the assignment manager. */
    private final Duration workerMetricsExpiry;

    /** Reads and heartbeats the leader version items. */
    @Getter
    private final LeaderVersionDAO leaderVersionDAO;

    /**
     * How long after its last report a worker's version hash (in its worker metrics) is considered stale. Short, so a
     * worker that stops (e.g. the last worker on the old version crashes) drops out of the per-version worker counts
     * quickly. Not used for {@code LeaderVersion}; see {@link #LEADER_VERSION_EXPIRY_MILLIS}.
     */
    private final long workerVersionExpiryMillis;

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
        this.workerVersionExpiryMillis = workerVersionExpiryMillis(config);
        this.leaseDurationMillis = config.failoverTimeMillis();
        this.workerMetricsExpiry = workerMetricsExpiry(config);
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
        this.workerVersionExpiryMillis = workerVersionExpiryMillis(config);
        this.leaseDurationMillis = config.failoverTimeMillis();
        this.workerMetricsExpiry = workerMetricsExpiry(config);
        this.leaderVersionDAO = leaderVersionDAO;
    }

    /** Matches MigrationAwareLAMDataManager: a worker is gone after 2 missed metrics reports. */
    private static Duration workerMetricsExpiry(final LeaseManagementConfig config) {
        return Duration.ofMillis(
                2 * config.workerUtilizationAwareAssignmentConfig().workerMetricsReporterFreqInMillis());
    }

    /**
     * Sets how the flip to the Leader lock is re-validated. Called by the Scheduler once the worker metrics DAO and
     * lease refresher exist.
     */
    public void setFleetViewReader(final FleetViewReader fleetViewReader) {
        this.fleetViewReader = fleetViewReader;
    }

    /**
     * The worker version expiry is the max of two leader heartbeats or two cycles of workers emitting metrics, which is
     * the current expiry threshold for workers.
     */
    private static long workerVersionExpiryMillis(final LeaseManagementConfig config) {
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
     * <p>{@code LeaderVersion} counts as current for {@link #LEADER_VERSION_EXPIRY_MILLIS} after its last heartbeat, so
     * a Leader holder that stops heartbeating (e.g. crashes) does not immediately send deploying-version workers to the
     * Leader lock. A missing item, one without a version, or one older than that, sends every worker to Leader.</p>
     *
     * @return Key of the leader lock (either "Leader" or "DeployingLeader").
     * @throws VersionStateUnavailableException if the read fails and no previous read has succeeded.
     */
    public String getHashKeyForLeaderLock() {
        return getHashKeyForLeaderLock(isEnabled());
    }

    /**
     * Same as {@link #getHashKeyForLeaderLock()}, with whether segmenting is enabled supplied by the caller. The leader
     * decider reads it once per leadership check and passes the same value here and to
     * {@link #setHeldLeaderKey(String, boolean)}, so the key and the version write can't disagree.
     */
    public String getHashKeyForLeaderLock(final boolean segmentingEnabled) {
        // If segmenting handler is disabled, default to returning the Leader key
        if (!segmentingEnabled) {
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
        final long nowMillis = System.currentTimeMillis();
        // No current version (first rollout, segmenting never used, or the last Leader stopped heartbeating more than
        // LEADER_VERSION_EXPIRY_MILLIS ago): every worker takes the Leader lock.
        if (currentVersion == null
                || currentVersion.getVersionHash() == null
                || currentVersion.isExpired(nowMillis, LEADER_VERSION_EXPIRY_MILLIS)) {
            log.info(
                    "Returning {}: {} is missing, has no version, or expired. currentVersion={}",
                    LeaderLock.LEADER_HASH_KEY,
                    LeaderVersionState.LEADER_VERSION_HASH_KEY,
                    currentVersion);
            return LeaderLock.LEADER_HASH_KEY;
        }

        // If all workers are emitting the same version, take the Leader lock.
        // Otherwise take the leader lock depending on the version hash.
        final boolean versionHashMatches = versionHash.equals(currentVersion.getVersionHash());
        if (versionHashMatches) {
            return LeaderLock.LEADER_HASH_KEY;
        }
        if (isVersionEmittedByAllActiveWorkers) {
            // This is the flip: this worker's version becomes the fleet's current version. The flag is from the
            // last assignment run, which may be up to a heartbeat period old, so a worker on the old version that
            // came back since (e.g. a rollback) would be missed. Re-check against a fresh read first.
            if (isFlipStillSafe(nowMillis)) {
                log.info(
                        "All active workers report version {} and own its leases; taking {}.",
                        versionHash,
                        LeaderLock.LEADER_HASH_KEY);
                return LeaderLock.LEADER_HASH_KEY;
            }
            return LeaderLock.DEPLOYING_LEADER_HASH_KEY;
        }
        // The current Leader has not heartbeated for longer than a worker would take to go stale, so it has probably
        // stopped. Say why this worker stays away from the Leader lock.
        if (currentVersion.isExpired(nowMillis, workerVersionExpiryMillis)) {
            log.info(
                    "{} (version {}) has not been heartbeated for {}ms; staying on {} until a worker on that version "
                            + "takes {} or all active workers report version {}.",
                    LeaderVersionState.LEADER_VERSION_HASH_KEY,
                    currentVersion.getVersionHash(),
                    nowMillis - currentVersion.getHeartbeatMillis(),
                    LeaderLock.DEPLOYING_LEADER_HASH_KEY,
                    LeaderLock.LEADER_HASH_KEY,
                    versionHash);
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
        // Whoever holds DeployingLeader is never current, even if segmenting is no longer enabled for it (e.g. its
        // write routing diverged from its lock client when table migration completed). Otherwise it would run the
        // current Leader's duties, such as stream sync, alongside the real Leader until its key is re-evaluated.
        if (LeaderLock.DEPLOYING_LEADER_HASH_KEY.equals(currentlyHeldLeaderKey)) {
            return false;
        }
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
        setHeldLeaderKey(key, isEnabled());
    }

    /**
     * Same as {@link #setHeldLeaderKey(String)}, with whether segmenting is enabled supplied by the caller: the same
     * value the caller used to choose the key (see {@link #getHashKeyForLeaderLock(boolean)}). A key chosen while
     * disabled is never passed to the {@link LeaderVersionDAO}, so it can't write a version item.
     */
    public void setHeldLeaderKey(final String key, final boolean segmentingEnabled) {
        leaderVersionDAO.setHeldLockKey(segmentingEnabled ? key : null);
        currentlyHeldLeaderKey = key;
        if (key != null) {
            lastHeldLeaderKey = key;
        } else {
            // The flag is only computed by a leader's assignment run. Once this worker holds no lock it is never
            // refreshed, so clear it rather than keep steering this worker to the Leader key on an old observation.
            isVersionEmittedByAllActiveWorkers = false;
            allOnVersionSnapshot = null;
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
        setIsVersionEmittedByAllActiveWorkers(activeWorkerMetrics, workersOnVersionHash, Collections.emptyList());
    }

    /**
     * Same as {@link #setIsVersionEmittedByAllActiveWorkers(List, List)}, also recording each lease's owner and
     * counter from the same run. The flip to the Leader lock compares them with a fresh read to tell a live owner on
     * another version (its lease counter moved) from a dead one.
     */
    public void setIsVersionEmittedByAllActiveWorkers(
            final List<WorkerMetricStats> activeWorkerMetrics,
            final List<WorkerMetricStats> workersOnVersionHash,
            final List<Lease> leases) {
        final boolean allOnVersion = !activeWorkerMetrics.isEmpty()
                && toWorkerIds(activeWorkerMetrics).equals(toWorkerIds(workersOnVersionHash));
        if (allOnVersion) {
            // Keep the first snapshot of an unbroken run of "all on version", so its age measures how long the
            // fleet has looked that way.
            final AllOnVersionSnapshot previous = allOnVersionSnapshot;
            if (previous == null || !isVersionEmittedByAllActiveWorkers) {
                allOnVersionSnapshot = AllOnVersionSnapshot.of(System.currentTimeMillis(), leases);
            }
        } else {
            allOnVersionSnapshot = null;
        }
        isVersionEmittedByAllActiveWorkers = allOnVersion;
    }

    /**
     * Re-validates the flip with a fresh read of the worker metrics and leases:
     * <ul>
     *   <li>every active worker (fresh metrics row) reports this worker's version, and there is at least one;</li>
     *   <li>every lease owned by a worker not on this version is held by a dead owner: same owner and same lease
     *       counter as when the fleet was first seen all on this version, at least a lease duration ago. A worker
     *       that came back on the old version renews its lease (the counter moves) before its metrics row is
     *       fresh again, so this catches it.</li>
     * </ul>
     * Fails closed: a read error, a snapshot younger than a lease duration, or no reader set means "not yet".
     */
    private boolean isFlipStillSafe(final long nowMillis) {
        final AllOnVersionSnapshot snapshot = allOnVersionSnapshot;
        if (snapshot == null) {
            return false;
        }
        if (nowMillis - snapshot.takenAtMillis < leaseDurationMillis) {
            log.info(
                    "All active workers report version {}; confirming for {}ms before taking {}.",
                    versionHash,
                    leaseDurationMillis - (nowMillis - snapshot.takenAtMillis),
                    LeaderLock.LEADER_HASH_KEY);
            return false;
        }
        final FleetViewReader reader = fleetViewReader;
        if (reader == null) {
            log.warn("No fresh fleet view available; not taking {}.", LeaderLock.LEADER_HASH_KEY);
            return false;
        }
        final FleetView fresh;
        try {
            fresh = reader.read();
        } catch (final Exception e) {
            log.warn(
                    "Failed to read the fleet to confirm the flip; not taking {} this time.",
                    LeaderLock.LEADER_HASH_KEY,
                    e);
            return false;
        }
        final List<WorkerMetricStats> active = fresh.getWorkerMetrics().stream()
                .filter(wm -> wm.isValidWorkerMetric() && !wm.isExpired(workerMetricsExpiry))
                .collect(Collectors.toList());
        final Set<String> onVersion = toWorkerIds(filterWorkersOnVersionHash(active));
        final Set<String> activeIds = toWorkerIds(active);
        if (activeIds.isEmpty() || !onVersion.equals(activeIds)) {
            log.info(
                    "Fresh read: active workers {} but on version {} only {}; not taking {}.",
                    activeIds,
                    versionHash,
                    onVersion,
                    LeaderLock.LEADER_HASH_KEY);
            return false;
        }
        for (final Lease lease : fresh.getLeases()) {
            final String owner = lease.leaseOwner();
            if (owner == null || onVersion.contains(owner)) {
                continue;
            }
            final LeaseOwnerAndCounter before = snapshot.leases.get(lease.leaseKey());
            if (before == null
                    || !owner.equals(before.owner)
                    || !Objects.equals(lease.leaseCounter(), before.counter)) {
                log.info(
                        "Fresh read: lease {} is owned by {}, not on version {}, and was renewed or moved; not "
                                + "taking {}.",
                        lease.leaseKey(),
                        owner,
                        versionHash,
                        LeaderLock.LEADER_HASH_KEY);
                return false;
            }
        }
        return true;
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
        return Duration.between(Instant.ofEpochSecond(lut), Instant.now()).toMillis() > workerVersionExpiryMillis;
    }

    private boolean doesWorkerHaveVersionHash(final WorkerMetricStats worker) {
        return worker.getProperties() != null && worker.getProperties().containsKey(VERSION_HASH_KEY);
    }

    /** Fresh worker metrics and leases, read to re-validate the flip. */
    @lombok.Value
    public static class FleetView {
        List<WorkerMetricStats> workerMetrics;
        List<Lease> leases;
    }

    /** Reads a fresh {@link FleetView}. */
    @FunctionalInterface
    public interface FleetViewReader {
        FleetView read() throws Exception;
    }

    @lombok.Value
    private static class LeaseOwnerAndCounter {
        String owner;
        Long counter;
    }

    private static final class AllOnVersionSnapshot {
        private final long takenAtMillis;
        private final Map<String, LeaseOwnerAndCounter> leases;

        private AllOnVersionSnapshot(final long takenAtMillis, final Map<String, LeaseOwnerAndCounter> leases) {
            this.takenAtMillis = takenAtMillis;
            this.leases = leases;
        }

        static AllOnVersionSnapshot of(final long takenAtMillis, final List<Lease> leases) {
            final Map<String, LeaseOwnerAndCounter> byKey = new HashMap<>();
            for (final Lease lease : leases) {
                byKey.put(lease.leaseKey(), new LeaseOwnerAndCounter(lease.leaseOwner(), lease.leaseCounter()));
            }
            return new AllOnVersionSnapshot(takenAtMillis, byKey);
        }
    }
}
