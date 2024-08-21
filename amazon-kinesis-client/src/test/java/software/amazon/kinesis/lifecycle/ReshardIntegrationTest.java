package software.amazon.kinesis.lifecycle;

import org.junit.Test;
import software.amazon.kinesis.application.TestConsumer;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.ReleaseCanaryStreamingReshardingTestConfig;

public class ReshardIntegrationTest {
    @Test
    public void kclReleaseCanaryStreamingReshardingTest() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryStreamingReshardingTestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }
}
