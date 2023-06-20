package software.amazon.kinesis.lifecycle;

import org.junit.Test;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.ReleaseCanaryPollingH2TestConfig;
import software.amazon.kinesis.config.ReleaseCanaryPollingH1TestConfig;
import software.amazon.kinesis.config.ReleaseCanaryStreamingTestConfig;
import software.amazon.kinesis.utils.TestConsumer;

public class BasicStreamConsumerIntegrationTest {

    @Test
    public void kclReleaseCanaryPollingH2Test() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryPollingH2TestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

//    @Test
//    public void kclReleaseCanaryPollingH1Test() throws Exception {
//        KCLAppConfig consumerConfig = new ReleaseCanaryPollingH1TestConfig();
//        TestConsumer consumer = new TestConsumer(consumerConfig);
//        consumer.run();
//    }
//
//    @Test
//    public void kclReleaseCanaryStreamingTest() throws Exception {
//        KCLAppConfig consumerConfig = new ReleaseCanaryStreamingTestConfig();
//        TestConsumer consumer = new TestConsumer(consumerConfig);
//        consumer.run();
//    }
}
