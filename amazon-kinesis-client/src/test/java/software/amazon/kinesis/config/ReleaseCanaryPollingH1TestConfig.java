package software.amazon.kinesis.config;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Config for a polling consumer with HTTP protocol of HTTP1
 */
public class ReleaseCanaryPollingH1TestConfig extends KCLAppConfig {

    private final UUID uniqueId = UUID.randomUUID();

    private final String applicationName = "PollingH1Test";
    private final String streamName = "2XPollingH1TestStream_" + uniqueId;

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
        return Protocol.HTTP1_1;
    }

    @Override
    public RetrievalMode getRetrievalMode() {
        return RetrievalMode.POLLING;
    }
}
