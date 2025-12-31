package software.amazon.kinesis.application;

import org.junit.Test;
import software.amazon.kinesis.application.config.KCLAppConfig;
import software.amazon.kinesis.application.config.multistream.ReleaseCanaryMultiStreamPollingH2TestConfig;
import software.amazon.kinesis.application.config.multistream.ReleaseCanaryMultiStreamStreamingTestConfig;

public class MultiStreamConsumerIntegrationTest {
    @Test
    public void kclReleaseCanaryMultiStreamPollingTest() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryMultiStreamPollingH2TestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

    @Test
    public void kclReleaseCanaryMultiStreamStreamingTest() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryMultiStreamStreamingTestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }
}
