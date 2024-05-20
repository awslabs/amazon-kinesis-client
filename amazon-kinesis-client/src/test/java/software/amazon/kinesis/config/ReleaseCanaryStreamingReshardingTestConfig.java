package software.amazon.kinesis.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.utils.ReshardOptions;

import static software.amazon.kinesis.utils.ReshardOptions.MERGE;
import static software.amazon.kinesis.utils.ReshardOptions.SPLIT;

public class ReleaseCanaryStreamingReshardingTestConfig extends KCLAppConfig {

    private final UUID uniqueId = UUID.randomUUID();

    private final String applicationName = "StreamingReshardingTest";
    private final String streamName = "2XStreamingReshardingTestStream_" + uniqueId;

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

    @Override
    public int getShardCount() {
        return 20;
    }

    @Override
    public List<ReshardOptions> getReshardFactorList() {
        return Arrays.asList(SPLIT, MERGE);
    }
}
