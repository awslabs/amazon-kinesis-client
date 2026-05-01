package software.amazon.kinesis.segmenting;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

/**
 * Component responsible for handling the safe migration system. If enabled, workers that are getting deployed with
 * a new lease assignment algorithm will contend for the DeployingLeader lock. Workers that have the new algorithm will
 * have its leases balanced by the DeployingLeader. Leases owned by workers on the deploying version cannot be
 * transferred over unless the lease becomes unassigned.
 */
@Slf4j
@KinesisClientInternalApi
public class FleetSegmentingHandler {

    public static final String VERSION_HASH_KEY = "versionHash";
    private static final String VERSION_HASH_LUT_KEY = "versionHashLut";
    private static final long VERSION_HASH_EXPIRY_MILLIS = TimeUnit.HOURS.toMillis(1);

    @Getter
    private final String versionHash;

    @Getter
    private volatile boolean isVersionEmittedByAllActiveWorkers = false;

    @Getter
    private final boolean isEnabled;

    private final String leaderTableName;
    private final DynamoDbClient ddbClient;
    private final CoordinatorStateDAO coordinatorStateDAO;

    public FleetSegmentingHandler(
            final LeaseManagementConfig config,
            final DynamoDbClient ddbClient,
            final String leaderTableName,
            final CoordinatorStateDAO coordinatorStateDAO) {
        this.leaderTableName = leaderTableName;
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.ddbClient = ddbClient;
        this.versionHash = String.valueOf(config.leaseAssignmentStrategy().getVersionNum());
        isEnabled = config.enableSafeMigrationSystem();
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
        // If the current version does not exist, then this is the first time we are deploying versioned workers.
        // If the current version does exist but is expired, then the versioned workers were rolled back. For both
        // cases, obtain the normal leader lock.
        final Map<String, AttributeValue> currentVersionAttrs =
                getCoordinatorStateAttributes(CoordinatorState.LEADER_HASH_KEY);
        if (currentVersionAttrs == null || isVersionHashExpired(currentVersionAttrs)) {
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

    public boolean isOnCurrentVersion() {
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
        isVersionEmittedByAllActiveWorkers = activeWorkerMetrics.containsAll(workersOnVersionHash)
                && workersOnVersionHash.containsAll(activeWorkerMetrics);
    }

    private boolean isVersionHashExpired(final Map<String, AttributeValue> item) {
        return item.containsKey(VERSION_HASH_KEY)
                && item.containsKey(VERSION_HASH_LUT_KEY)
                && isVersionHashExpired(
                        Long.parseLong(item.get(VERSION_HASH_LUT_KEY).s()));
    }

    private boolean isVersionHashExpired(final long lut) {
        return Duration.between(Instant.ofEpochSecond(lut), Instant.now()).toMillis() > VERSION_HASH_EXPIRY_MILLIS;
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
