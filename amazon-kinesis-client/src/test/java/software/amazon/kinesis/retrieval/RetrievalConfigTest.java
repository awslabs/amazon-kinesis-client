package software.amazon.kinesis.retrieval;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.common.InitialPositionInStream.LATEST;
import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;

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
        createConfig(mockMultiStreamTracker).initialPositionInStreamExtended(
                InitialPositionInStreamExtended.newInitialPosition(TRIM_HORIZON));
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

    private RetrievalConfig createConfig(StreamTracker streamTracker) {
        return new RetrievalConfig(mockKinesisClient, streamTracker, APPLICATION_NAME);
    }

}