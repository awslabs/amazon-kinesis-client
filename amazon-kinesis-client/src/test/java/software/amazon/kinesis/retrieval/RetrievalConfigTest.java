package software.amazon.kinesis.retrieval;

import java.util.Arrays;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.common.InitialPositionInStream.LATEST;
import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;

@RunWith(MockitoJUnitRunner.class)
public class RetrievalConfigTest {

    private static final String APPLICATION_NAME = RetrievalConfigTest.class.getSimpleName();

    @Mock
    private KinesisAsyncClient mockKinesisClient;

    @Mock
    private MultiStreamTracker mockMultiStreamTracker;

    @Before
    public void setUp() {
        when(mockMultiStreamTracker.isMultiStream()).thenReturn(true);
    }

    @Test
    public void testSingleStreamTrackerConstruction() {
        final String streamName = "single-stream";
        final Arn streamArn = createArn(streamName);

        for (final RetrievalConfig rc : Arrays.asList(
                createConfig(streamName),
                createConfig(new SingleStreamTracker(streamName)),
                createConfig(streamArn),
                createConfig(new SingleStreamTracker(streamArn)))) {
            assertEquals(Optional.empty(), rc.appStreamTracker().left());
            assertEquals(
                    streamName,
                    rc.streamTracker()
                            .streamConfigList()
                            .get(0)
                            .streamIdentifier()
                            .streamName());
            assertEquals(1, rc.streamTracker().streamConfigList().size());
            assertFalse(rc.streamTracker().isMultiStream());
        }
    }

    @Test
    public void testMultiStreamTrackerConstruction() {
        final StreamTracker mockMultiStreamTracker = mock(MultiStreamTracker.class);
        final RetrievalConfig configByMultiTracker = createConfig(mockMultiStreamTracker);
        assertEquals(Optional.empty(), configByMultiTracker.appStreamTracker().right());
        assertEquals(
                mockMultiStreamTracker,
                configByMultiTracker.appStreamTracker().left().get());
        assertEquals(mockMultiStreamTracker, configByMultiTracker.streamTracker());
    }

    @Test
    public void testUpdateInitialPositionInSingleStream() {
        final RetrievalConfig config = createConfig(new SingleStreamTracker("foo"));

        for (final StreamConfig sc : config.streamTracker().streamConfigList()) {
            assertEquals(LATEST, sc.initialPositionInStreamExtended().getInitialPositionInStream());
        }
        config.initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPosition(TRIM_HORIZON));
        for (final StreamConfig sc : config.streamTracker().streamConfigList()) {
            assertEquals(TRIM_HORIZON, sc.initialPositionInStreamExtended().getInitialPositionInStream());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateInitialPositionInMultiStream() {
        createConfig(mockMultiStreamTracker)
                .initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPosition(TRIM_HORIZON));
    }

    /**
     * Test that an invalid {@link RetrievalSpecificConfig} does not overwrite
     * a valid one.
     */
    @Test
    public void testInvalidRetrievalSpecificConfig() {
        final RetrievalSpecificConfig validConfig = mock(RetrievalSpecificConfig.class);
        final RetrievalSpecificConfig invalidConfig = mock(RetrievalSpecificConfig.class);
        doThrow(new IllegalArgumentException("womp womp")).when(invalidConfig).validateState(true);

        final RetrievalConfig config = createConfig(mockMultiStreamTracker);
        assertNull(config.retrievalSpecificConfig());
        config.retrievalSpecificConfig(validConfig);
        assertEquals(validConfig, config.retrievalSpecificConfig());

        try {
            config.retrievalSpecificConfig(invalidConfig);
            Assert.fail("should throw");
        } catch (RuntimeException re) {
            assertEquals(validConfig, config.retrievalSpecificConfig());
        }
    }

    private RetrievalConfig createConfig(String streamName) {
        return new RetrievalConfig(mockKinesisClient, streamName, APPLICATION_NAME);
    }

    private RetrievalConfig createConfig(Arn streamArn) {
        return new RetrievalConfig(mockKinesisClient, streamArn, APPLICATION_NAME);
    }

    private RetrievalConfig createConfig(StreamTracker streamTracker) {
        return new RetrievalConfig(mockKinesisClient, streamTracker, APPLICATION_NAME);
    }

    private static Arn createArn(String streamName) {
        return Arn.builder()
                .partition("aws")
                .service("kinesis")
                .region(Region.US_EAST_1.id())
                .accountId("123456789012")
                .resource("stream/" + streamName)
                .build();
    }
}
