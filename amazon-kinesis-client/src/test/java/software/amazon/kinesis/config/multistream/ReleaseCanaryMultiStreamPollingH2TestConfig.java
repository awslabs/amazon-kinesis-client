package software.amazon.kinesis.config.multistream;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.RetrievalMode;

/**
 * Config for a polling consumer with HTTP protocol of HTTP2
 */
public class ReleaseCanaryMultiStreamPollingH2TestConfig extends KCLAppConfig {

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
            streamArns.add(buildStreamArn(String.join("_", streamName, i.toString())));
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
