package software.amazon.kinesis.config.multistream;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.RetrievalMode;

public class ReleaseCanaryMultiStreamStreamingTestConfig extends KCLAppConfig {
    private final UUID uniqueId = UUID.randomUUID();
    private final int numStreams = 2;
    private final String applicationName = "MultiStreamStreamingTest";
    private final String streamName = "2XMultiStreamStreamingTestStream";

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
