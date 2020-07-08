package software.amazon.kinesis.retrieval;

import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.List;

public class DataRetrievalUtil {

    public static boolean isValidResult(String shardEndIndicator, List<ChildShard> childShards) {
        // shardEndIndicator is nextShardIterator for GetRecordsResponse, and is continuationSequenceNumber for SubscribeToShardEvent
        // There are two valid scenarios for the shardEndIndicator and childShards combination.
        // 1. ShardEnd scenario: shardEndIndicator should be null and childShards should be a non-empty list.
        // 2. Non-ShardEnd scenario: shardEndIndicator should be non-null and childShards should be null or an empty list.
        // Otherwise, the retrieval result is invalid.
        if (shardEndIndicator == null && CollectionUtils.isNullOrEmpty(childShards) ||
                shardEndIndicator != null && !CollectionUtils.isNullOrEmpty(childShards)) {
            return false;
        }

        // For ShardEnd scenario, for each childShard we should validate if parentShards are available.
        // Missing parentShards can cause issues with creating leases for childShards during ShardConsumer shutdown.
        if (!CollectionUtils.isNullOrEmpty(childShards)) {
            for (ChildShard childShard : childShards) {
                if (CollectionUtils.isNullOrEmpty(childShard.parentShards())) {
                    return false;
                }
            }
        }
        return true;
    }
}
