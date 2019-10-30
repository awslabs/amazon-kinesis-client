package software.amazon.kinesis.utils;

import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

public class SubscribeToShardRequestMatcher extends ArgumentMatcher<SubscribeToShardRequest> {

    private SubscribeToShardRequest left;
    public SubscribeToShardRequestMatcher(SubscribeToShardRequest left) {
        super();
        this.left = left;
    }

    public boolean matches(Object rightObject) {
        SubscribeToShardRequest right = (SubscribeToShardRequest)rightObject;
        return left.shardId().equals(right.shardId()) &&
                left.consumerARN().equals(right.consumerARN()) &&
                left.startingPosition().equals(right.startingPosition());
    }
}
