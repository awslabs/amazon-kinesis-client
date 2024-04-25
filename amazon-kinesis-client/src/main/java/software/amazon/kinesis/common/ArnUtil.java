package software.amazon.kinesis.common;

import lombok.NonNull;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

import static software.amazon.awssdk.services.kinesis.KinesisAsyncClient.SERVICE_NAME;

@KinesisClientInternalApi
public final class ArnUtil {
    private static final String STREAM_RESOURCE_PREFIX = "stream/";

    /**
     * Construct a Kinesis stream ARN.
     *
     * @param region The region the stream exists in.
     * @param accountId The account the stream belongs to.
     * @param streamName The name of the stream.
     * @return The {@link Arn} of the Kinesis stream.
     */
    public static Arn constructStreamArn(
            @NonNull final Region region, @NonNull final String accountId, @NonNull final String streamName) {
        return Arn.builder()
                .partition(region.metadata().partition().id())
                .service(SERVICE_NAME)
                .region(region.id())
                .accountId(accountId)
                .resource(STREAM_RESOURCE_PREFIX + streamName)
                .build();
    }
}
