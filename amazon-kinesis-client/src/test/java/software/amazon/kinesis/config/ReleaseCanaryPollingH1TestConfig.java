package software.amazon.kinesis.config;

import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

/**
 * Config for a polling consumer with HTTP protocol of HTTP1
 */
public class ReleaseCanaryPollingH1TestConfig extends KCLAppConfig {

    private final UUID uniqueId = UUID.randomUUID();

    @Override
    public String getStreamName() {
        return "KCLReleaseCanary2XPollingH1TestStream_" + uniqueId;
    }

    @Override
    public Protocol getKinesisClientProtocol() {
        return Protocol.HTTP1_1;
    }

    @Override
    public RetrievalConfig getRetrievalConfig() throws IOException, URISyntaxException {

        final InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended
                .newInitialPosition(getInitialPosition());

        final RetrievalConfig config = getConfigsBuilder().retrievalConfig();
        config.initialPositionInStreamExtended(initialPosition);
        config.retrievalSpecificConfig(new PollingConfig(getStreamName(), config.kinesisClient()));

        return config;
    }
}
