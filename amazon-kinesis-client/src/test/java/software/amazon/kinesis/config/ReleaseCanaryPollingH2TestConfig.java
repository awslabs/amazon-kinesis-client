package software.amazon.kinesis.config;

import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Config for a polling consumer with HTTP protocol of HTTP2
 */
public class ReleaseCanaryPollingH2TestConfig extends KCLAppConfig {
    @Override
    public String getStreamName() {
        return "KCLReleaseCanary2XPollingH2TestStream";
    }

    @Override
    public Protocol getConsumerProtocol() {
        return Protocol.HTTP2;
    }

    @Override
    public RetrievalConfig getRetrievalConfig() throws IOException, URISyntaxException {

        InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended
                .newInitialPosition(getInitialPosition());

        RetrievalConfig config = getConfigsBuilder().retrievalConfig();
        config.initialPositionInStreamExtended(initialPosition);
        config.retrievalSpecificConfig(new PollingConfig(getStreamName(), buildConsumerClient()));

        return config;
    }
}

