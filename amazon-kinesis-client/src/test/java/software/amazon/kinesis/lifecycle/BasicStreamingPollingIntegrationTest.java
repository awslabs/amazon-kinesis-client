package software.amazon.kinesis.lifecycle;

import org.junit.Test;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.ReleaseCanaryPollingH1TestConfig;
import software.amazon.kinesis.config.ReleaseCanaryPollingH2TestConfig;
import software.amazon.kinesis.config.ReleaseCanaryStreamingTestConfig;
import software.amazon.kinesis.utils.TestConsumer;

public class BasicStreamingPollingIntegrationTest {

    @Test
    public void KCLReleaseCanaryPollingH2Test() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryPollingH2TestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

    @Test
    public void KCLReleaseCanaryPollingH1Test() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryPollingH1TestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

    @Test
    public void KCLReleaseCanaryStreamingTest() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryStreamingTestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }
}
