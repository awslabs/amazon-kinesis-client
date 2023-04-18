package software.amazon.kinesis.retrieval.polling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

@RunWith(MockitoJUnitRunner.class)
public class PollingConfigTest {

    private static final String STREAM_NAME = PollingConfigTest.class.getSimpleName();

    @Mock
    private KinesisAsyncClient mockKinesisClinet;

    private PollingConfig config;

    @Before
    public void setUp() {
        config = new PollingConfig(mockKinesisClinet);
    }

    @Test
    public void testValidState() {
        assertNull(config.streamName());

        config.validateState(true);
        config.validateState(false);

        config.streamName(STREAM_NAME);
        config.validateState(false);
        assertEquals(STREAM_NAME, config.streamName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStateMultiWithStreamName() {
        config.streamName(STREAM_NAME);

        config.validateState(true);
    }

}