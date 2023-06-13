package software.amazon.kinesis.config;

import lombok.Value;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.utils.RecordValidatorQueue;
import software.amazon.kinesis.utils.TestRecordProcessorFactory;
import lombok.Builder;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
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
import java.util.Optional;

public interface KCLAppConfig {

    String getStreamName();

    default String getStreamArn() {
        return null;
    }

    int getShardCount();

    String getApplicationName();

    String getEndpoint();

    Region getRegion();


    /**
     * "default" profile, should match with profiles listed in "cat ~/.aws/config"
     */
    String getProfile();

    InitialPositionInStream getInitialPosition();

    Protocol getConsumerProtocol();

    Protocol getProducerProtocol();

    ProducerConfig getProducerConfig();

    ReshardConfig getReshardConfig();

    default KinesisAsyncClient buildConsumerClient() throws URISyntaxException, IOException {
        return buildAsyncKinesisClient(getConsumerProtocol());
    }

    default KinesisAsyncClient buildProducerClient() throws URISyntaxException, IOException {
        return buildAsyncKinesisClient(getProducerProtocol());
    }

    default KinesisAsyncClient buildAsyncKinesisClient(Protocol protocol) throws URISyntaxException, IOException {
        return buildAsyncKinesisClient(Optional.ofNullable(protocol));
    }

    default KinesisAsyncClient buildAsyncKinesisClient(Optional<Protocol> protocol) throws URISyntaxException, IOException {

        /**
         * Setup H2 client config.
         */
        final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(Integer.MAX_VALUE);

        /**
         * If not present, defaults to HTTP1_1
         */
        if (protocol.isPresent()) {
            builder.protocol(protocol.get());
        }

        final SdkAsyncHttpClient sdkAsyncHttpClient =
                builder.buildWithDefaults(AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true).build());

        /**
         * Setup client builder by default values
         */
        final KinesisAsyncClientBuilder kinesisAsyncClientBuilder = KinesisAsyncClient.builder().region(getRegion());

        kinesisAsyncClientBuilder.httpClient(sdkAsyncHttpClient);

        AwsCredentialsProvider credentialsProvider = (getProfile() != null) ?
                ProfileCredentialsProvider.builder().profileName(getProfile()).build() : DefaultCredentialsProvider.create();
        kinesisAsyncClientBuilder.credentialsProvider( credentialsProvider );

        return kinesisAsyncClientBuilder.build();
    }

    default DynamoDbAsyncClient buildAsyncDynamoDbClient() throws IOException {
        final DynamoDbAsyncClientBuilder builder = DynamoDbAsyncClient.builder().region(getRegion());

        AwsCredentialsProvider credentialsProvider = (getProfile() != null) ?
                ProfileCredentialsProvider.builder().profileName(getProfile()).build() : DefaultCredentialsProvider.create();
        builder.credentialsProvider(credentialsProvider);

        return builder.build();
    }

    default CloudWatchAsyncClient buildAsyncCloudWatchClient() throws IOException {
        final CloudWatchAsyncClientBuilder builder = CloudWatchAsyncClient.builder().region(getRegion());

        AwsCredentialsProvider credentialsProvider = (getProfile() != null) ?
                ProfileCredentialsProvider.builder().profileName(getProfile()).build() : DefaultCredentialsProvider.create();
        builder.credentialsProvider(credentialsProvider);

        return builder.build();
    }

    default String getWorkerId() throws UnknownHostException {
        return Inet4Address.getLocalHost().getHostName();
    }

    default RecordValidatorQueue getRecordValidator() {
        return new RecordValidatorQueue();
    }

    default ShardRecordProcessorFactory getShardRecordProcessorFactory() {
        return new TestRecordProcessorFactory(getRecordValidator());
    }

    default ConfigsBuilder getConfigsBuilder() throws IOException, URISyntaxException {
        return getConfigsBuilder("");
    }

    default ConfigsBuilder getConfigsBuilder(String workerIdSuffix) throws IOException, URISyntaxException {
        final String workerId = getWorkerId() + workerIdSuffix;
        if (getStreamArn() == null) {
            return new ConfigsBuilder(getStreamName(), getApplicationName(), buildConsumerClient(), buildAsyncDynamoDbClient(),
                    buildAsyncCloudWatchClient(), workerId, getShardRecordProcessorFactory());
        } else {
            return new ConfigsBuilder(Arn.fromString(getStreamArn()), getApplicationName(), buildConsumerClient(), buildAsyncDynamoDbClient(),
                    buildAsyncCloudWatchClient(), workerId, getShardRecordProcessorFactory());
        }
    }

    RetrievalConfig getRetrievalConfig() throws IOException, URISyntaxException;

    /**
     * Configure ingress load (batch size, record size, and calling interval)
     */
    @Value
    @Builder
    class ProducerConfig {
        private boolean isBatchPut;
        private int batchSize;
        private int recordSizeKB;
        private long callPeriodMills;
    }

    /**
     * Description of the method of resharding for a test case
     * <p>
     * reshardingFactorCycle: lists the scales by which the number of shards in a stream will be updated
     * in sequence. e.g {2.0, 0.5} means that the number of shards will first be doubled, then halved
     * <p>
     * numReshardCycles: the number of resharding cycles that will be executed in a test]
     * <p>
     * reshardFrequencyMillis: the period of time between reshard cycles (in milliseconds)
     */
    @Value
    @Builder
    class ReshardConfig {
        private double[] reshardingFactorCycle;
        private int numReshardCycles;
        private long reshardFrequencyMillis;
    }

}
