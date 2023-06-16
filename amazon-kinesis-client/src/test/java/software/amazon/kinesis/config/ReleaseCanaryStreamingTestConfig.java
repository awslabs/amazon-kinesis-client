package software.amazon.kinesis.config;

import software.amazon.awssdk.http.Protocol;

/**
 * Config for a streaming consumer with HTTP protocol of HTTP2
 */
public class ReleaseCanaryStreamingTestConfig extends KCLAppConfig {
    @Override
    public String getStreamName() {
        return "KCLReleaseCanary2XStreamingTestStream";
    }

    @Override
    public Protocol getConsumerProtocol() {
        return Protocol.HTTP2;
    }

}

