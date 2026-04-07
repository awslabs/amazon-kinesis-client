package software.amazon.kinesis.segmenting;

import java.util.Optional;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import lombok.Getter;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.LeaseManagementConfig;

@KinesisClientInternalApi
public class FleetSegmentingHandler {

    @Getter
    private final String versionHash;

    @Getter
    private final String versionHashDDBKey = "versionHash";

    public FleetSegmentingHandler(final LeaseManagementConfig config) {
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
    public String getHashKeyForLeaderLock(final AmazonDynamoDBLockClient lockClient) {
        final Optional<LockItem> leaderLock = lockClient.getLock(CoordinatorState.LEADER_HASH_KEY, Optional.empty());
        if (!leaderLock.isPresent()
                || (leaderLock.get().getAdditionalAttributes().containsKey(versionHashDDBKey)
                        && leaderLock
                                .get()
                                .getAdditionalAttributes()
                                .get(versionHashDDBKey)
                                .s()
                                .equals(versionHash))) {
            return CoordinatorState.LEADER_HASH_KEY;
        }
        return CoordinatorState.DEPLOYING_LEADER_HASH_KEY;
    }
}
