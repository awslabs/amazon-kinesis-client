package software.amazon.kinesis.integration_tests;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import software.amazon.kinesis.config.KCLAppConfig;

@Slf4j
public class KCL2XIntegrationTest {

    private static final String CONFIG_PACKAGE = "software.amazon.kinesis.config";

    @Test
    public void KCLReleaseCanary2XPollingH2Test() throws Exception {
        String[] configName = { "KCLReleaseCanary2XPollingH2TestConfig" };
        KCLAppConfig consumerConfig = (KCLAppConfig) Class.forName(CONFIG_PACKAGE + "." + configName[0]).newInstance();
        TestConsumerV2 consumer = new TestConsumerV2( consumerConfig );
        consumer.run();
    }

    @Test
    public void KCLReleaseCanary2XPollingH1Test() throws Exception {
        String[] configName = { "KCLReleaseCanary2XPollingH1TestConfig" };
        KCLAppConfig consumerConfig = (KCLAppConfig) Class.forName(CONFIG_PACKAGE + "." + configName[0]).newInstance();
        TestConsumerV2 consumer = new TestConsumerV2( consumerConfig );
        consumer.run();
    }

    @Test
    public void KCLReleaseCanary2XStreamingTest() throws Exception {
        String[] configName = { "KCLReleaseCanary2XStreamingTestConfig" };
        KCLAppConfig consumerConfig = (KCLAppConfig) Class.forName(CONFIG_PACKAGE + "." + configName[0]).newInstance();
        TestConsumerV2 consumer = new TestConsumerV2( consumerConfig );
        consumer.run();
    }
}
