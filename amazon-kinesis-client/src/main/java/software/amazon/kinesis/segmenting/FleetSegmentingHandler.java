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
     * Returns the version hash concatenated with the last updated time. The epoch is appended to the version hash
     * to determine if the version hash of a worker is outdated.
     * @return String in the format of {versionHash}:{lastUpdatedTime}
     */
    public String getVersionHashWithLastUpdatedTime(final long epoch) {
        return String.format("%s:%s", versionHash, epoch);
    }

    /**
     * Returns the key of the leader lock to take depending on the worker's own version hash.
     * @return Key of the leader lock (either "Leader" or "DeployingLeader").
     */
    public String getHashKeyForLeaderLock() {
        final GetItemRequest getLeaderItemRequest = GetItemRequest.builder()
                .tableName(leaderTableName)
                .key(Collections.singletonMap(
                        CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME,
                        AttributeValue.fromS(CoordinatorState.LEADER_HASH_KEY)))
                .build();
        final GetItemResponse getLeaderItemResponse = ddbClient.getItem(getLeaderItemRequest);

        if (!getLeaderItemResponse.hasItem() || doesVersionHashMatchLeaderVersionHash(getLeaderItemResponse)) {
            return CoordinatorState.LEADER_HASH_KEY;
        }
        return CoordinatorState.DEPLOYING_LEADER_HASH_KEY;
    }

    private boolean doesVersionHashMatchLeaderVersionHash(final GetItemResponse response) {
        if (response.hasItem() && response.item().containsKey(versionHashDDBKey)) {
            return versionHash.equals(response.item().get(versionHashDDBKey).s());
        }
        return false;
    }
}
