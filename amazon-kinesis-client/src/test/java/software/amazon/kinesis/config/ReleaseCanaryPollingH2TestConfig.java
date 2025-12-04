package software.amazon.kinesis.config;

import java.util.Collections;
import java.util.List;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;

/**
 * Config for a polling consumer with HTTP protocol of HTTP2
 */
public class ReleaseCanaryPollingH2TestConfig extends KCLAppConfig {

    private final String applicationName = "PollingH2Test";
    private final String streamName = "PollingH2TestStream";

    @Override
    public String getTestName() {
        return applicationName;
    }

    @Override
    public List<Arn> getStreamArns() {
        return Collections.singletonList(buildStreamArn(streamName));
    }

    @Override
    public Protocol getKinesisClientProtocol() {
        return Protocol.HTTP2;
    }

    @Override
    public RetrievalMode getRetrievalMode() {
        return RetrievalMode.POLLING;
    }
}
