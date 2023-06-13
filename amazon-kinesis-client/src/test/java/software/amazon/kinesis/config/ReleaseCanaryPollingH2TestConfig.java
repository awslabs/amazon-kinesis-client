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
 * Config for a polling consumer with HTTP protocol of HTTP2
 */
public class ReleaseCanaryPollingH2TestConfig extends BasicReleaseCanaryConfig {
    @Override
    public String getStreamName() {
        return "KCLTest3";
    }

    @Override
    public int getShardCount() {
        return 20;
    }

    @Override
    public String getApplicationName() {
        return "KCLReleaseCanary2XPollingH2TestApplication";
    }

    @Override
    public Protocol getConsumerProtocol() {
        return Protocol.HTTP2;
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

