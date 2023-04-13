package software.amazon.kinesis.retrieval;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static software.amazon.kinesis.common.InitialPositionInStream.LATEST;
import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.utils.MockObjectHelper;

public class RetrievalConfigTest {

    private static final String APPLICATION_NAME = RetrievalConfigTest.class.getSimpleName();

    private KinesisAsyncClient mockKinesisClient;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mockKinesisClient = MockObjectHelper.createKinesisClient(true);
    }

    @Test
    public void testTrackerConstruction() {
        final String streamName = "single-stream";
        final RetrievalConfig configByName = createConfig(streamName);
        final SingleStreamTracker singleTracker = new SingleStreamTracker(streamName);
        final RetrievalConfig configBySingleTracker = createConfig(singleTracker);

        for (final RetrievalConfig rc : Arrays.asList(configByName, configBySingleTracker)) {
            assertEquals(Optional.empty(), rc.appStreamTracker().left());
            assertEquals(singleTracker, rc.streamTracker());
            assertEquals(1, rc.streamTracker().streamConfigList().size());
            assertFalse(rc.streamTracker().isMultiStream());
        }

        final StreamTracker mockMultiStreamTracker = mock(MultiStreamTracker.class);
        final RetrievalConfig configByMultiTracker = createConfig(mockMultiStreamTracker);
        assertEquals(Optional.empty(), configByMultiTracker.appStreamTracker().right());
        assertEquals(mockMultiStreamTracker, configByMultiTracker.appStreamTracker().left().get());
        assertEquals(mockMultiStreamTracker, configByMultiTracker.streamTracker());
    }

    @Test
    public void testUpdateInitialPositionInSingleStream() {
        final RetrievalConfig config = createConfig(new SingleStreamTracker("foo"));

        for (final StreamConfig sc : config.streamTracker().streamConfigList()) {
            assertEquals(LATEST, sc.initialPositionInStreamExtended().getInitialPositionInStream());
        }
        config.initialPositionInStreamExtended(
                InitialPositionInStreamExtended.newInitialPosition(TRIM_HORIZON));
        for (final StreamConfig sc : config.streamTracker().streamConfigList()) {
            assertEquals(TRIM_HORIZON, sc.initialPositionInStreamExtended().getInitialPositionInStream());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateInitialPositionInMultiStream() {
        final RetrievalConfig config = createConfig(mock(MultiStreamTracker.class));
        config.initialPositionInStreamExtended(
                InitialPositionInStreamExtended.newInitialPosition(TRIM_HORIZON));
    }

    private RetrievalConfig createConfig(String streamName) {
        return new RetrievalConfig(mockKinesisClient, streamName, APPLICATION_NAME);
    }

    private RetrievalConfig createConfig(StreamTracker streamTracker) {
        return new RetrievalConfig(mockKinesisClient, streamTracker, APPLICATION_NAME);
    }

}