package software.amazon.kinesis.segmenting;

import java.util.Collections;

import lombok.Getter;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.LeaseManagementConfig;

@KinesisClientInternalApi
public class FleetSegmentingHandler {

    @Getter
    private final String versionHash;

    @Getter
    private final String versionHashDDBKey = "versionHash";

    private final String leaderTableName;

    private final DynamoDbClient ddbClient;

    public FleetSegmentingHandler(
            final LeaseManagementConfig config, final DynamoDbClient ddbClient, final String leaderTableName) {
        this.leaderTableName = leaderTableName;
        this.ddbClient = ddbClient;
        versionHash = String.valueOf(config.leaseAssignmentMetric().name().hashCode());
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

    public boolean isWorkerOnDeployingVersion() {
        final GetItemResponse getLeaderItemResponse = getLeaderForHashKey(CoordinatorState.DEPLOYING_LEADER_HASH_KEY);
        return doesVersionHashMatchLeaderVersionHash(getLeaderItemResponse);
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
        if (getLeaderItemResponse.hasItem() && getLeaderItemResponse.item().containsKey(versionHashDDBKey)) {
            return versionHash.equals(
                    getLeaderItemResponse.item().get(versionHashDDBKey).s());
        }
        return false;
    }
}
