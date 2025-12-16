package software.amazon.kinesis.common;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

import static software.amazon.awssdk.services.kinesis.KinesisAsyncClient.SERVICE_NAME;

public class DefaultKinesisArnConstructor implements ArnConstructor {

    /**
     * {@inheritDoc}
     */
    @Override
    public Arn constructStreamArn(Region region, String accountId, String streamName) {
        return Arn.builder()
                .partition(region.metadata().partition().id())
                .service(SERVICE_NAME)
                .region(region.id())
                .accountId(accountId)
                .resource(STREAM_RESOURCE_PREFIX + streamName)
                .build();
    }
}
