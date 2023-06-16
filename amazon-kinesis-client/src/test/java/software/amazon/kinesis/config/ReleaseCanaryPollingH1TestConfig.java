package software.amazon.kinesis.config;

import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Config for a polling consumer with HTTP protocol of HTTP1
 */
public class ReleaseCanaryPollingH1TestConfig extends KCLAppConfig {
    @Override
    public String getStreamName() {
        return "KCLReleaseCanary2XPollingH1TestStream";
    }

    @Override
    public Protocol getConsumerProtocol() {
        return Protocol.HTTP1_1;
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
