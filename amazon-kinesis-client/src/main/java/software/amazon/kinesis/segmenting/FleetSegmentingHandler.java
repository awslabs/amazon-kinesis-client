package software.amazon.kinesis.segmenting;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
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
import software.amazon.kinesis.leases.LeaseManagementConfig;
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

    public FleetSegmentingHandler(final LeaseManagementConfig config, final CoordinatorStateDAO coordinatorStateDAO) {
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.versionHash = String.valueOf(config.leaseAssignmentStrategy().getVersionNum());
        isEnabled = config.enableRollingDeploymentSystem();
        workerId = config.workerIdentifier();

        // The expiry threshold will be the max of two leader heartbeats or two cycles of workers emitting metrics,
        // which is the current expiry threshold for workers.
        this.versionHashExpiryMillis = Math.max(
                2 * config.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis(),
                2 * config.workerUtilizationAwareAssignmentConfig().workerMetricsReporterFreqInMillis());

        startVersionHashHeartbeat(config.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis());
    }

    /**
     * Returns the key of the leader lock to take depending on whether the worker is part of the deploying version.
     * @return Key of the leader lock (either "Leader" or "DeployingLeader").
     */
    public String getHashKeyForLeaderLock() {
        // If segmenting handler is disabled, default to returning the Leader key
        if (!isEnabled) {
            return CoordinatorState.LEADER_HASH_KEY;
        }
        // If the current leader does not exist, default to obtaining the Leader lock.
        // If the current leader does exist but is expired, then the versioned workers were rolled back. For both
        // cases, obtain the normal leader lock.
        final Map<String, AttributeValue> currentVersionAttrs =
                getCoordinatorStateAttributes(CoordinatorState.LEADER_HASH_KEY);
        if (currentVersionAttrs == null
                || !currentVersionAttrs.containsKey(VERSION_HASH_KEY)
                || isVersionHashExpired(currentVersionAttrs)) {
            return CoordinatorState.LEADER_HASH_KEY;
        }

        // If all workers are emitting the same version, take the Leader lock.
        // If the current version does exist and the version hash is not expired, take the leader lock depending on
        // the value of the version hash.
        if (isVersionEmittedByAllActiveWorkers || doesVersionHashMatch(currentVersionAttrs)) {
            return CoordinatorState.LEADER_HASH_KEY;
        }
        return CoordinatorState.DEPLOYING_LEADER_HASH_KEY;
    }

    public Map<String, String> getVersionHashWithLastUpdatedTime() {
        final Map<String, String> workerProperties = new HashMap<>();
        workerProperties.put(VERSION_HASH_KEY, versionHash);
        workerProperties.put(VERSION_HASH_LUT_KEY, String.valueOf(Instant.now().getEpochSecond()));
        return workerProperties;
    }

    public Map<String, AttributeValue> getVersionHashWithLastUpdatedTimeForLockTable() {
        Map<String, AttributeValue> workerProperties = new HashMap<>();
        getVersionHashWithLastUpdatedTime().forEach((k, v) -> workerProperties.put(k, AttributeValue.fromS(v)));
        return workerProperties;
    }

    /**
     * Used by the leader to check if it should handle the current Leader responsibilities (as opposed to the
     * DeployingLeader responsibilities). If the segmenting handler is disabled, the leader will behave as it normally
     * would without segmenting.
     */
    public boolean isOnCurrentVersion() {
        if (!isEnabled) {
            return true;
        }
        final Map<String, AttributeValue> attrs = getCoordinatorStateAttributes(CoordinatorState.LEADER_HASH_KEY);
        return doesVersionHashMatch(attrs);
    }

    public boolean isOnDeployingVersion() {
        final Map<String, AttributeValue> attrs =
                getCoordinatorStateAttributes(CoordinatorState.DEPLOYING_LEADER_HASH_KEY);
        return doesVersionHashMatch(attrs);
    }

    public boolean isWorkerVersionHashStale(final WorkerMetricStats worker) {
        if (worker.getProperties() == null) {
            return true;
        }
        String lutStr = worker.getProperties().get(VERSION_HASH_LUT_KEY);
        if (lutStr == null) {
            return true;
        }
        return isVersionHashExpired(Long.parseLong(lutStr));
    }

    public void setIsVersionEmittedByAllActiveWorkers(
            final List<WorkerMetricStats> activeWorkerMetrics, final List<WorkerMetricStats> workersOnVersionHash) {
        isVersionEmittedByAllActiveWorkers =
                new HashSet<>(activeWorkerMetrics).equals(new HashSet<>(workersOnVersionHash));
    }

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

    private void startVersionHashHeartbeat(final long leaderHeartbeatMillis) {
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this::updateLeaderVersionHashLut, 0, leaderHeartbeatMillis, TimeUnit.MILLISECONDS);
    }

    private void updateLeaderVersionHashLut() {
        // checking if worker is the leader to avoid unnecessary calls to DDB
        if (!isLeader) {
            return;
        }
        final String leaderKey = getHashKeyForLeaderLock();
        final Map<String, ExpectedAttributeValue> expectations = getVersionHashHeartbeatExpectationMap(leaderKey);
        final CoordinatorState state = CoordinatorState.builder()
                .key(leaderKey)
                .attributes(Collections.singletonMap(
                        VERSION_HASH_LUT_KEY,
                        AttributeValue.fromS(String.valueOf(Instant.now().getEpochSecond()))))
                .build();
        try {
            coordinatorStateDAO.updateCoordinatorStateWithExpectation(state, expectations);
        } catch (final Exception e) {
            log.error("Failed to update leader versionHashLut", e);
        }
    }

    private Map<String, ExpectedAttributeValue> getVersionHashHeartbeatExpectationMap(final String leaderKey) {
        final Map<String, ExpectedAttributeValue> expectations = new HashMap<>();
        expectations.put(
                CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME,
                ExpectedAttributeValue.builder()
                        .value(AttributeValue.fromS(leaderKey))
                        .build());
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

    private Map<String, AttributeValue> getCoordinatorStateAttributes(final String key) {
        try {
            final CoordinatorState state = coordinatorStateDAO.getCoordinatorState(key);
            return state != null ? state.getAttributes() : null;
        } catch (final Exception e) {
            log.error("Failed to get coordinator state for key {}", key, e);
            return null;
        }
    }

    private boolean doesVersionHashMatch(final Map<String, AttributeValue> attrs) {
        return attrs != null
                && attrs.containsKey(VERSION_HASH_KEY)
                && versionHash.equals(attrs.get(VERSION_HASH_KEY).s());
    }
}
