package software.amazon.kinesis.utils;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class MockObjectHelper {

    public static KinesisAsyncClient createKinesisClient() {
        return createKinesisClient(Region.US_EAST_1);
    }

    /**
     * @param isRegionDummy a boolean to determine whether to use a null value for the Kinesis client's region.
     * @return
     */
    public static KinesisAsyncClient createKinesisClient(boolean isRegionDummy) {
        return isRegionDummy ? createKinesisClient(null) : createKinesisClient();
    }

    public static KinesisAsyncClient createKinesisClient(Region region) {
        KinesisAsyncClient kinesisClient = mock(KinesisAsyncClient.class);
        when(kinesisClient.serviceClientConfiguration()).
                thenReturn(KinesisServiceClientConfiguration.builder().region(region).build());
        return kinesisClient;
    }

}
