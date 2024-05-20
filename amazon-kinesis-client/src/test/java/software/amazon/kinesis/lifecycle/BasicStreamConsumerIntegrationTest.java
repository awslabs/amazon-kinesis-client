package software.amazon.kinesis.lifecycle;

import org.junit.Test;
import software.amazon.kinesis.application.TestConsumer;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.ReleaseCanaryPollingH1TestConfig;
import software.amazon.kinesis.config.ReleaseCanaryPollingH2TestConfig;
import software.amazon.kinesis.config.ReleaseCanaryStreamingTestConfig;

public class BasicStreamConsumerIntegrationTest {

    /**
     * Test with a polling consumer using HTTP2 protocol.
     * In the polling case, consumer makes calls to the producer each time to request records to process.
     */
    @Test
    public void kclReleaseCanaryPollingH2Test() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryPollingH2TestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

    /**
     * Test with a polling consumer using HTTP1 protocol.
     * In the polling case, consumer makes calls to the producer each time to request records to process.
     */
    @Test
    public void kclReleaseCanaryPollingH1Test() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryPollingH1TestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

    /**
     * Test with a streaming consumer.
     * In the streaming configuration, connection is made once between consumer and producer and producer continuously sends data to be processed.
     */
    @Test
    public void kclReleaseCanaryStreamingTest() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryStreamingTestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }
}
