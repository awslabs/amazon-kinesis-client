package software.amazon.kinesis.application;

import org.junit.Ignore;
import org.junit.Test;
import software.amazon.kinesis.application.config.KCLAppConfig;
import software.amazon.kinesis.application.config.ReleaseCanaryStreamingReshardingTestConfig;

@Ignore
public class ReshardIntegrationTest {
    @Test
    public void kclReleaseCanaryStreamingReshardingTest() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryStreamingReshardingTestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }
}
