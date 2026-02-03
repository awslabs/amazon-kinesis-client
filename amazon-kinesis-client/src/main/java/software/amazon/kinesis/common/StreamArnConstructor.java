package software.amazon.kinesis.common;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

public interface StreamArnConstructor {
    String STREAM_RESOURCE_PREFIX = "stream/";

    /**
     * Construct a Kinesis stream ARN.
     *
     * @param region The region the stream exists in.
     * @param accountId The account the stream belongs to.
     * @param streamName The name of the stream.
     * @return The {@link Arn} of the Kinesis stream.
     */
    Arn constructStreamArn(Region region, String accountId, String streamName);
}
