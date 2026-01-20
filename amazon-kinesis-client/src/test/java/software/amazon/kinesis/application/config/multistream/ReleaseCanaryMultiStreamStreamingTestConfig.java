package software.amazon.kinesis.application.config.multistream;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.application.config.KCLAppConfig;
import software.amazon.kinesis.application.config.RetrievalMode;

public class ReleaseCanaryMultiStreamStreamingTestConfig extends KCLAppConfig {
    private final int numStreams = 2;
    private final String applicationName = "MultiStreamStreamingTest";
    private final String streamName = "MultiStreamStreamingTestStream";

    @Override
    public String getTestName() {
        return applicationName;
    }

    @Override
    public List<Arn> getStreamArns() {
        ArrayList<Arn> streamArns = new ArrayList<>(numStreams);
        for (int i = 1; i <= numStreams; i++) {
            streamArns.add(buildStreamArn(String.join("_", streamName, Integer.toString(i))));
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
