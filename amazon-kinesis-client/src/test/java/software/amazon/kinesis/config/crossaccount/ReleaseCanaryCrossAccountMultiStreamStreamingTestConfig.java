package software.amazon.kinesis.config.crossaccount;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.config.RetrievalMode;

/**
 * Config for a cross account polling consumer with HTTP protocol of HTTP2
 */
@Slf4j
public class ReleaseCanaryCrossAccountMultiStreamStreamingTestConfig extends KCLCrossAccountAppConfig {
    private final UUID uniqueId = UUID.randomUUID();

    private final int numStreams = 2;
    private final String applicationName = "CrossAccountMultiStreamStreamingTest";

    private final String streamName = "2XCrossAccountStreamingTestStream";

    @Override
    public String getTestName() {
        return applicationName;
    }

    @Override
    public List<Arn> getStreamArns() {
        ArrayList<Arn> streamArns = new ArrayList<>(numStreams);
        for (int i = 1; i <= numStreams; i++) {
            streamArns.add(buildStreamArn(String.join("_", streamName, Integer.toString(i), uniqueId.toString())));
        }
        return streamArns;
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
