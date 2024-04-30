package software.amazon.kinesis.config;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Config for a polling consumer with HTTP protocol of HTTP2
 */
public class ReleaseCanaryMultiStreamPollingH2TestConfig extends KCLAppConfig {
    private final UUID uniqueId = UUID.randomUUID();

    private final int numStreams = 2;
    private final String applicationName = "MultiStreamPollingH2Test";
    private final String streamName = "2XMultiStreamPollingH2TestStream";

    @Override
    public String getTestName() {
        return applicationName;
    }

    @Override
    public List<Arn> getStreamArns() {
        ArrayList<Arn> streamArns = new ArrayList<>(numStreams);
        for (Integer i = 1; i <= numStreams; i++) {
            streamArns.add(buildStreamArn(String.join("_", streamName, i.toString(), uniqueId.toString())));
        }
        return streamArns;
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
