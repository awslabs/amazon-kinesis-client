package software.amazon.kinesis.config;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Config for a streaming consumer with HTTP protocol of HTTP2
 */
public class ReleaseCanaryStreamingTestConfig extends KCLAppConfig {
    private final UUID uniqueId = UUID.randomUUID();

    private final String applicationName = "StreamingTest";
    private final String streamName ="2XStreamingTestStream_" + uniqueId;

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
        return RetrievalMode.STREAMING;
    }
}
