package software.amazon.kinesis.config;

import lombok.Value;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.utils.RecordValidatorQueue;
import software.amazon.kinesis.utils.ReshardOptions;
import software.amazon.kinesis.utils.TestRecordProcessorFactory;
import lombok.Builder;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

/**
 * Default configuration for a producer or consumer used in integration tests.
 * Producer: puts records of size 60 KB at an interval of 100 ms
 * Consumer: streaming configuration (vs polling) that starts processing records at shard horizon
 */
public abstract class KCLAppConfig {

    private KinesisAsyncClient kinesisAsyncClient;
    private DynamoDbAsyncClient dynamoDbAsyncClient;
    private CloudWatchAsyncClient cloudWatchAsyncClient;
    private RecordValidatorQueue recordValidator;

    /**
     * Name used for test stream and lease tracker table
     */
    public abstract String getStreamName();

    public int getShardCount() { return 4; }

    public Region getRegion() { return Region.US_WEST_2; }

    /**
     * "default" profile, should match with profiles listed in "cat ~/.aws/config"
     */
    private AwsCredentialsProvider getCredentialsProvider() {
        final String awsProfile = System.getProperty("awsProfile");
        return (awsProfile != null) ?
                ProfileCredentialsProvider.builder().profileName(awsProfile).build() : DefaultCredentialsProvider.create();
    }

    public InitialPositionInStream getInitialPosition() {
        return InitialPositionInStream.TRIM_HORIZON;
    }

    public abstract Protocol getKinesisClientProtocol();

    public ProducerConfig getProducerConfig() {
        return ProducerConfig.builder()
                .isBatchPut(false)
                .batchSize(1)
                .recordSizeKB(60)
                .callPeriodMills(100)
                .build();
    }

    public ReshardConfig getReshardConfig() {
        return null;
    }

    public final KinesisAsyncClient buildAsyncKinesisClient() throws URISyntaxException, IOException {
        if (kinesisAsyncClient == null) {
            // Setup H2 client config.
            final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(Integer.MAX_VALUE);

            builder.protocol(getKinesisClientProtocol());

            final SdkAsyncHttpClient sdkAsyncHttpClient =
                    builder.buildWithDefaults(AttributeMap.builder().build());

            // Setup client builder by default values
            final KinesisAsyncClientBuilder kinesisAsyncClientBuilder = KinesisAsyncClient.builder().region(getRegion());

            kinesisAsyncClientBuilder.httpClient(sdkAsyncHttpClient);

            kinesisAsyncClientBuilder.credentialsProvider(getCredentialsProvider());

            this.kinesisAsyncClient = kinesisAsyncClientBuilder.build();
        }

        return this.kinesisAsyncClient;
    }

    public final DynamoDbAsyncClient buildAsyncDynamoDbClient() throws IOException {
        if (this.dynamoDbAsyncClient == null) {
            final DynamoDbAsyncClientBuilder builder = DynamoDbAsyncClient.builder().region(getRegion());
            builder.credentialsProvider(getCredentialsProvider());
            this.dynamoDbAsyncClient = builder.build();
        }
        return this.dynamoDbAsyncClient;
    }

    public final CloudWatchAsyncClient buildAsyncCloudWatchClient() throws IOException {
        if (this.cloudWatchAsyncClient == null) {
            final CloudWatchAsyncClientBuilder builder = CloudWatchAsyncClient.builder().region(getRegion());
            builder.credentialsProvider(getCredentialsProvider());
            this.cloudWatchAsyncClient = builder.build();
        }
        return this.cloudWatchAsyncClient;
    }

    public final String getWorkerId() throws UnknownHostException {
        return Inet4Address.getLocalHost().getHostName();
    }

    public final RecordValidatorQueue getRecordValidator() {
        if (recordValidator == null) {
            this.recordValidator = new RecordValidatorQueue();
        }
        return this.recordValidator;
    }

    public ShardRecordProcessorFactory getShardRecordProcessorFactory() {
        return new TestRecordProcessorFactory(getRecordValidator());
    }

    public final ConfigsBuilder getConfigsBuilder() throws IOException, URISyntaxException {
        final String workerId = getWorkerId();
        return new ConfigsBuilder(getStreamName(), getStreamName(), buildAsyncKinesisClient(), buildAsyncDynamoDbClient(),
                buildAsyncCloudWatchClient(), workerId, getShardRecordProcessorFactory());
    }

    public RetrievalConfig getRetrievalConfig() throws IOException, URISyntaxException {
        final InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended
                .newInitialPosition(getInitialPosition());

        // Default is a streaming consumer
        final RetrievalConfig config = getConfigsBuilder().retrievalConfig();
        config.initialPositionInStreamExtended(initialPosition);
        return config;
    }

    /**
     * Configure ingress load (batch size, record size, and calling interval)
     */
    @Value
    @Builder
    static class ProducerConfig {
        private boolean isBatchPut;
        private int batchSize;
        private int recordSizeKB;
        private long callPeriodMills;
    }

    /**
     * Description of the method of resharding for a test case
     */
    @Value
    @Builder
    static class ReshardConfig {
        /**
         * reshardingFactorCycle: lists the order or reshards that will be done during one reshard cycle
         * e.g {SPLIT, MERGE} means that the number of shards will first be doubled, then halved
         */
        private ReshardOptions[] reshardingFactorCycle;

        /**
         * numReshardCycles: the number of resharding cycles that will be executed in a test
         */
        private int numReshardCycles;

        /**
         * reshardFrequencyMillis: the period of time between reshard cycles (in milliseconds)
         */
        private long reshardFrequencyMillis;
    }

}
