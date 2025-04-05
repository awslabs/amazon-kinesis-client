package com.amazonaws.services.kinesis.clientlibrary.lifecycle;

import com.amazonaws.services.kinesis.clientlibrary.config.ReleaseCanaryPollingTestConfig;
import com.amazonaws.services.kinesis.clientlibrary.utils.TestConsumer;
import org.junit.Test;

public class BasicPollingIntegrationTest {

    /**
     * Test with a polling consumer.
     * In the polling case, consumer makes calls to the producer each time to request records to process.
     */
    @Test
    public void kclReleaseCanaryPollingTest() throws Exception {
        ReleaseCanaryPollingTestConfig consumerConfig = new ReleaseCanaryPollingTestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

}
