package software.amazon.kinesis.config.crossaccount;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.config.RetrievalMode;

/**
 * Config for a streaming consumer with HTTP protocol of HTTP2
 */
@Slf4j
public class ReleaseCanaryCrossAccountStreamingTestConfig extends KCLCrossAccountAppConfig {
    private final UUID uniqueId = UUID.randomUUID();

    private final String applicationName = "CrossAccountStreamingTest";
    private final String streamName = "2XCrossAccountStreamingTestStream_" + uniqueId;

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
