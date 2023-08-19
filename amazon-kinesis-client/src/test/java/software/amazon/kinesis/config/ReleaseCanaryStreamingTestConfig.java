package software.amazon.kinesis.config;

import software.amazon.awssdk.http.Protocol;

import java.util.UUID;

/**
 * Config for a streaming consumer with HTTP protocol of HTTP2
 */
public class ReleaseCanaryStreamingTestConfig extends KCLAppConfig {
    private final UUID uniqueId = UUID.randomUUID();

    @Override
    public String getStreamName() {
        return "KCLReleaseCanary2XStreamingTestStream_" + uniqueId;
    }

    @Override
    public Protocol getKinesisClientProtocol() {
        return Protocol.HTTP2;
    }

}

