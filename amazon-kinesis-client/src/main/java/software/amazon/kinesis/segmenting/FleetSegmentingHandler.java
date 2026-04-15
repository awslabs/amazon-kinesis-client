package software.amazon.kinesis.segmenting;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

@KinesisClientInternalApi
public class FleetSegmentingHandler {

    @Getter
    private final String versionHash;

    @Getter
    private final String versionHashKey = "versionHash";

    private boolean isVersionEmittedByAllActiveWorkers = false;
    private final long versionHashExpiryMillis;
    private final String versionHashLutKey = "versionHashLut";
    private final String leaderTableName;
    private final DynamoDbClient ddbClient;

    public FleetSegmentingHandler(
            final LeaseManagementConfig config, final DynamoDbClient ddbClient, final String leaderTableName) {
        this.leaderTableName = leaderTableName;
        this.ddbClient = ddbClient;
        this.versionHash = String.valueOf(config.leaseAssignmentMetric().name().hashCode());
        this.versionHashExpiryMillis = TimeUnit.HOURS.toMillis(1);
    }

    /**
     * Returns the key of the leader lock to take depending on the worker's own version hash.
     * @return Key of the leader lock (either "Leader" or "DeployingLeader").
     */
    public String getHashKeyForLeaderLock() {
        final GetItemResponse getLeaderItemResponse = getLeaderForHashKey(CoordinatorState.LEADER_HASH_KEY);
        if (!doesLeaderHaveValidVersion(getLeaderItemResponse)
                || doesVersionHashMatchLeaderVersionHash(getLeaderItemResponse)
                || isVersionEmittedByAllActiveWorkers) {
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

    public boolean isOnCurrentVersion() {
        final GetItemResponse getLeaderItemResponse = getLeaderForHashKey(CoordinatorState.LEADER_HASH_KEY);
        return doesVersionHashMatchLeaderVersionHash(getLeaderItemResponse);
    }

    public boolean isOnDeployingVersion() {
        final GetItemResponse getLeaderItemResponse = getLeaderForHashKey(CoordinatorState.DEPLOYING_LEADER_HASH_KEY);
        return doesVersionHashMatchLeaderVersionHash(getLeaderItemResponse);
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

    public boolean doesDeployingLeaderHaveValidVersion() {
        return doesLeaderHaveValidVersion(getLeaderForHashKey(CoordinatorState.DEPLOYING_LEADER_HASH_KEY));
    }

    private boolean doesLeaderHaveValidVersion(final GetItemResponse getLeaderItemResponse) {
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

    private GetItemResponse getLeaderForHashKey(final String leaderHashKey) {
        final GetItemRequest getLeaderItemRequest = GetItemRequest.builder()
                .tableName(leaderTableName)
                .key(Collections.singletonMap(
                        CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME,
                        AttributeValue.fromS(leaderHashKey)))
                .build();
        return ddbClient.getItem(getLeaderItemRequest);
    }

    private boolean doesVersionHashMatchLeaderVersionHash(final GetItemResponse getLeaderItemResponse) {
        if (getLeaderItemResponse.hasItem() && getLeaderItemResponse.item().containsKey(versionHashKey)) {
            return versionHash.equals(
                    getLeaderItemResponse.item().get(versionHashKey).s());
        }
        return false;
    }
}
