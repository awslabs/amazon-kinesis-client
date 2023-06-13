package software.amazon.kinesis.config;

import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * Config for a polling consumer with HTTP protocol of HTTP1
 */
public class ReleaseCanaryPollingH1TestConfig extends BasicReleaseCanaryConfig {
    @Override
    public String getStreamName() {
        return "KCLReleaseCanary2XPollingH1TestConfig";
    }

    @Override
    public int getShardCount() {
        return 20;
    }

    @Override
    public String getApplicationName() {
        return "KCLReleaseCanary2XPollingH1TestConfigApplication";
    }

    @Override
    public Protocol getConsumerProtocol() {
        return Protocol.HTTP1_1;
    }

    @Override
    public RetrievalConfig getRetrievalConfig() throws IOException, URISyntaxException {
        LocalDateTime d = LocalDateTime.now();
        d = d.minusMinutes(5);
        Instant instant = d.atZone(ZoneId.systemDefault()).toInstant();
        Date startStreamTime = Date.from(instant);

        InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended
                .newInitialPositionAtTimestamp(startStreamTime);

        RetrievalConfig config = getConfigsBuilder().retrievalConfig();
        config.initialPositionInStreamExtended(initialPosition);
        config.retrievalSpecificConfig(new PollingConfig(getStreamName(), buildConsumerClient()));

        return config;
    }
}
