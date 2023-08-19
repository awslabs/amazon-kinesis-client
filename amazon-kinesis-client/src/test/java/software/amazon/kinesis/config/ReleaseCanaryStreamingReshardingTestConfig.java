package software.amazon.kinesis.config;

import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.utils.ReshardOptions;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static software.amazon.kinesis.utils.ReshardOptions.MERGE;
import static software.amazon.kinesis.utils.ReshardOptions.SPLIT;

public class ReleaseCanaryStreamingReshardingTestConfig extends KCLAppConfig {

    private final UUID uniqueId = UUID.randomUUID();
    @Override
    public String getStreamName() {
        return "KCLReleaseCanary2XStreamingReshardingTestStream_" + uniqueId;
    }

    @Override
    public Protocol getKinesisClientProtocol() { return Protocol.HTTP2; }

    @Override
    public int getShardCount() {
        return 100;
    }

    @Override
    public List<ReshardOptions> getReshardFactorList() {
        return Arrays.asList(SPLIT, MERGE);
    }

}
