package software.amazon.kinesis.config;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.retrieval.RetrievalConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * Basic config for a release canary (streaming) with default settings
 */
@Slf4j
public class BasicReleaseCanaryConfig implements KCLAppConfig {
    @Override
    public String getStreamName() {
        return "";
    }

    @Override
    public int getShardCount() {
        return 10;
    }

    @Override
    public String getApplicationName() {
        return "";
    }

    @Override
    public String getEndpoint() {
        return "";
    }

    @Override
    public Region getRegion() {
        return Region.US_WEST_2;
    }


    /**
     * This will get the credentials that are provided in the maven command
     * when running integration tests if any are provided through -Dcredentials=iamUser
     * Otherwise, iamUser will be null and the test will search for default credentials
     * in the test environment.
     */
    @Override
    public String getProfile() {
        String iamUser = System.getProperty("credentials");
        return iamUser;
    }

    @Override
    public InitialPositionInStream getInitialPosition() {
        return InitialPositionInStream.TRIM_HORIZON;
    }

    @Override
    public Protocol getConsumerProtocol() {
        return Protocol.HTTP1_1;
    }

    @Override
    public Protocol getProducerProtocol() {
        return Protocol.HTTP1_1;
    }

    @Override
    public ProducerConfig getProducerConfig() {
        return ProducerConfig.builder()
                .isBatchPut(false)
                .batchSize(1)
                .recordSizeKB(60)
                .callPeriodMills(100)
                .build();
    }

    @Override
    public ReshardConfig getReshardConfig() {
        return null;
    }

    @Override
    public RetrievalConfig getRetrievalConfig() throws IOException, URISyntaxException {
        LocalDateTime d = LocalDateTime.now();
        d = d.minusMinutes(5);
        Instant instant = d.atZone(ZoneId.systemDefault()).toInstant();
        Date startStreamTime = Date.from(instant);

        InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended
                .newInitialPositionAtTimestamp(startStreamTime);

        /**
         * Default is a streaming consumer
         */
        RetrievalConfig config = getConfigsBuilder().retrievalConfig();
        config.initialPositionInStreamExtended(initialPosition);

        return config;
    }
}
