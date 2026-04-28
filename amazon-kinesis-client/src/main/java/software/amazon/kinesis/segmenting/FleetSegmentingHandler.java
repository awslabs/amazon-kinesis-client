package software.amazon.kinesis.segmenting;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
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

    private static final String CURRENT_VERSION_KEY = "CurrentVersion";

    @Getter
    private final String versionHash;

    @Getter
    private final String versionHashKey = "versionHash";

    @Getter
    private boolean isVersionEmittedByAllActiveWorkers = false;

    @Getter
    private final boolean isEnabled;

    private final long versionHashExpiryMillis = TimeUnit.HOURS.toMillis(1);
    private final String versionHashLutKey = "versionHashLut";
    private final String leaderTableName;
    private final DynamoDbClient ddbClient;

    public FleetSegmentingHandler(
            final LeaseManagementConfig config, final DynamoDbClient ddbClient, final String leaderTableName) {
        this.leaderTableName = leaderTableName;
        this.ddbClient = ddbClient;
        this.versionHash =
                String.valueOf(config.leaseAssignmentStrategy().name().hashCode());
        isEnabled = config.enableSafeMigrationSystem();
    }

    /**
     * Returns the key of the leader lock to take depending on whether the worker is part of the deploying version.
     * @return Key of the leader lock (either "Leader" or "DeployingLeader").
     */
    public String getHashKeyForLeaderLock() {
        final GetItemResponse currentVersionResponse = getItemFromCoordinatorTable(CURRENT_VERSION_KEY);
        if (!isEnabled
                || !currentVersionResponse.hasItem()
                || (isVersionHashValid(currentVersionResponse) && doesVersionHashMatch(currentVersionResponse))) {
            return CoordinatorState.LEADER_HASH_KEY;
        }
        return CoordinatorState.DEPLOYING_LEADER_HASH_KEY;
    }

    public Map<String, String> getVersionHashWithLastUpdatedTime() {
        final Map<String, String> workerProperties = new HashMap<>();
        workerProperties.put(versionHashKey, versionHash);
        workerProperties.put(versionHashLutKey, String.valueOf(Instant.now().getEpochSecond()));
        return workerProperties;
    }

    public Map<String, AttributeValue> getVersionHashWithLastUpdatedTimeForLockTable() {
        Map<String, AttributeValue> workerProperties = new HashMap<>();
        getVersionHashWithLastUpdatedTime().forEach((k, v) -> workerProperties.put(k, AttributeValue.fromS(v)));
        return workerProperties;
    }

    public boolean isOnCurrentVersion() {
        final GetItemResponse getLeaderItemResponse = getItemFromCoordinatorTable(CoordinatorState.LEADER_HASH_KEY);
        return doesVersionHashMatch(getLeaderItemResponse);
    }

    public boolean isOnDeployingVersion() {
        final GetItemResponse getLeaderItemResponse =
                getItemFromCoordinatorTable(CoordinatorState.DEPLOYING_LEADER_HASH_KEY);
        return doesVersionHashMatch(getLeaderItemResponse);
    }

    public boolean isWorkerVersionHashStale(final WorkerMetricStats worker) {
        if (worker.getProperties() == null) {
            return true;
        }
        String lutStr = worker.getProperties().get(versionHashLutKey);
        if (lutStr == null) {
            return true;
        }
        return isVersionHashExpired(Long.parseLong(lutStr));
    }

    public void setIsVersionEmittedByAllActiveWorkers(
            final List<WorkerMetricStats> activeWorkerMetrics, final List<WorkerMetricStats> workersOnVersionHash) {
        isVersionEmittedByAllActiveWorkers = activeWorkerMetrics.size() == workersOnVersionHash.size();
    }

    /**
     * Update the CurrentVersion item in the Coordinator table with the current version hash and the last updated time.
     */
    public void updateCurrentVersion() {
        if (isVersionEmittedByAllActiveWorkers) {
            Map<String, AttributeValue> currentVersionMap = getVersionHashWithLastUpdatedTimeForLockTable();
            currentVersionMap.put("key", AttributeValue.fromS(CURRENT_VERSION_KEY));
            final PutItemRequest putItemRequest = PutItemRequest.builder()
                    .item(currentVersionMap)
                    .tableName(leaderTableName)
                    .build();
            ddbClient.putItem(putItemRequest);
        }
    }

    private boolean isVersionHashValid(final GetItemResponse getLeaderItemResponse) {
        if (!getLeaderItemResponse.hasItem()) {
            return false;
        }
        final Map<String, AttributeValue> item = getLeaderItemResponse.item();
        return item.containsKey(versionHashKey)
                && item.containsKey(versionHashLutKey)
                && !isVersionHashExpired(
                        Long.parseLong(item.get(versionHashLutKey).s()));
    }

    private boolean isVersionHashExpired(final long lut) {
        return Duration.between(Instant.ofEpochSecond(lut), Instant.now()).toMillis() > versionHashExpiryMillis;
    }

    private GetItemResponse getItemFromCoordinatorTable(final String key) {
        final GetItemRequest getLeaderItemRequest = GetItemRequest.builder()
                .tableName(leaderTableName)
                .key(Collections.singletonMap(
                        CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME, AttributeValue.fromS(key)))
                .build();
        return ddbClient.getItem(getLeaderItemRequest);
    }

    private boolean doesVersionHashMatch(final GetItemResponse getItemResponse) {
        return getItemResponse.hasItem()
                && getItemResponse.item().containsKey(versionHashKey)
                && versionHash.equals(getItemResponse.item().get(versionHashKey).s());
    }
}
