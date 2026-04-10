package software.amazon.kinesis.segmenting;

import java.util.Collections;
import java.util.Map;

import lombok.Getter;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;

@KinesisClientInternalApi
public class FleetSegmentingHandler {

    @Getter
    private final String versionHash;

    @Getter
    private final String versionHashKey = "versionHash";

    private final String leaderTableName;

    private final DynamoDbClient ddbClient;

    private final WorkerMetricStatsDAO workerMetricStatsDAO;

    public FleetSegmentingHandler(
            final LeaseManagementConfig config,
            final DynamoDbClient ddbClient,
            final String leaderTableName,
            final WorkerMetricStatsDAO workerMetricStatsDAO) {
        this.leaderTableName = leaderTableName;
        this.ddbClient = ddbClient;
        this.versionHash = String.valueOf(config.leaseAssignmentMetric().name().hashCode());
        this.workerMetricStatsDAO = workerMetricStatsDAO;
    }

    /**
     * Returns the key of the leader lock to take depending on the worker's own version hash.
     * @return Key of the leader lock (either "Leader" or "DeployingLeader").
     */
    public String getHashKeyForLeaderLock() {
        final GetItemResponse getLeaderItemResponse = getLeaderForHashKey(CoordinatorState.LEADER_HASH_KEY);
        if (!getLeaderItemResponse.hasItem() || doesVersionHashMatchLeaderVersionHash(getLeaderItemResponse)) {
            return CoordinatorState.LEADER_HASH_KEY;
        }
        return CoordinatorState.DEPLOYING_LEADER_HASH_KEY;
    }

    public Map<String, String> getVersionHashAsMap() {
        return Collections.singletonMap(versionHashKey, versionHash);
    }

    public boolean isOnCurrentVersion() {
        final GetItemResponse getLeaderItemResponse = getLeaderForHashKey(CoordinatorState.LEADER_HASH_KEY);
        return doesVersionHashMatchLeaderVersionHash(getLeaderItemResponse);
    }

    public boolean isOnDeployingVersion() {
        final GetItemResponse getLeaderItemResponse = getLeaderForHashKey(CoordinatorState.DEPLOYING_LEADER_HASH_KEY);
        return doesVersionHashMatchLeaderVersionHash(getLeaderItemResponse);
    }

    /**
     * Check that if all workers are emitting the deploying version. If this is true, the deploying leader becomes
     * the new leader. This operation is only performed by deploying leader.
     * @return
     */
    // TODO: make sure scans not performed very often as this will increase DDB costs.
    public boolean areAllWorkersEmittingDeployingVersion() {
        if (!isOnDeployingVersion()) {
            return false;
        }
        for (WorkerMetricStats worker : workerMetricStatsDAO.getAllWorkerMetricStats()) {
            if (!versionHash.equals(getVersionHashOfWorker(worker))) {
                return false;
            }
        }
        return true;
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

    private String getVersionHashOfWorker(WorkerMetricStats worker) {
        if (worker.getProperties() != null && worker.getProperties().containsKey(versionHashKey)) {
            return worker.getProperties().get(versionHashKey);
        }
        return null;
    }
}
