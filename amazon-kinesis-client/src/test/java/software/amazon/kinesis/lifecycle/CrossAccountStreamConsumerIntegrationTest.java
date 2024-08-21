package software.amazon.kinesis.lifecycle;

import org.junit.Test;
import software.amazon.kinesis.application.TestConsumer;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.crossaccount.ReleaseCanaryCrossAccountMultiStreamPollingH2TestConfig;
import software.amazon.kinesis.config.crossaccount.ReleaseCanaryCrossAccountMultiStreamStreamingTestConfig;
import software.amazon.kinesis.config.crossaccount.ReleaseCanaryCrossAccountPollingH2TestConfig;
import software.amazon.kinesis.config.crossaccount.ReleaseCanaryCrossAccountStreamingTestConfig;

public class CrossAccountStreamConsumerIntegrationTest {

    /**
     * Test with a cross account polling consumer using HTTP2 protocol.
     * In the polling case, consumer makes calls to the producer each time to request records to process.
     * The stream is in a different account than the kinesis client used to get records.
     */
    @Test
    public void kclReleaseCanaryCrossAccountPollingH2Test() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryCrossAccountPollingH2TestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

    @Test
    public void kclReleaseCanaryCrossAccountStreamingTest() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryCrossAccountStreamingTestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

    @Test
    public void kclReleaseCanaryCrossAccountMultiStreamStreamingTest() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryCrossAccountMultiStreamStreamingTestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }

    @Test
    public void kclReleaseCanaryCrossAccountMultiStreamPollingH2Test() throws Exception {
        KCLAppConfig consumerConfig = new ReleaseCanaryCrossAccountMultiStreamPollingH2TestConfig();
        TestConsumer consumer = new TestConsumer(consumerConfig);
        consumer.run();
    }
}
