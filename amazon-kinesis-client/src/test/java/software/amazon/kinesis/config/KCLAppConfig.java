package software.amazon.kinesis.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import software.amazon.kinesis.utils.RecordValidatorQueue;
import software.amazon.kinesis.integration_tests.TestRecordProcessorFactoryV2;
import software.amazon.kinesis.utils.KCLVersion;
import lombok.Builder;
import lombok.Data;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
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

    default KCLVersion getKCLVersion() {
        return KCLVersion.KCL2X;
    }

    default boolean canaryMonitorEnabled() {
        return false;
    }

    String getEndpoint();

    Region getRegion();

    boolean isProd();

    default boolean durabilityCheck() {
        return true;
    }

    // "default" profile, should match with profiles listed in "cat ~/.aws/config"
    String getProfile();

    // '-1' means round robin across 0, 5_000, 15_000, 30_000 milliseconds delay.
    // The delay period is picked according to current time, so expected to be unpredictable across different KCL runs.
    // '0' means PassThroughRecordProcessor
    // Any other constant will delay according to the specified value.
    long getProcessingDelayMillis();

    InitialPositionInStream getKclInitialPosition();

    Protocol getConsumerProtocol();

    Protocol getProducerProtocol();

    ProducerConfig getProducerConfig();

    ReshardConfig getReshardConfig();

    default MultiStreamRotatorConfig getMultiStreamRotatorConfig() {
        throw new UnsupportedOperationException();
    }

    default KinesisAsyncClient buildConsumerClient() throws URISyntaxException, IOException {
        return buildAsyncKinesisClient( getConsumerProtocol() );
    }

    default KinesisAsyncClient buildProducerClient() throws URISyntaxException, IOException {
        return buildAsyncKinesisClient( getProducerProtocol() );
    }

    default KinesisAsyncClient buildAsyncKinesisClient( Protocol protocol ) throws URISyntaxException, IOException {
        return buildAsyncKinesisClient( Optional.ofNullable( protocol ) );
    }

    default KinesisAsyncClient buildAsyncKinesisClient( Optional< Protocol > protocol ) throws URISyntaxException, IOException {

        // Setup H2 client config.
        final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder()
                .maxConcurrency( Integer.MAX_VALUE );

        if ( protocol.isPresent() ) {
            builder.protocol( protocol.get() );
        }

        final SdkAsyncHttpClient sdkAsyncHttpClient =
                builder.buildWithDefaults( AttributeMap.builder().put( SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true ).build() );

        // Setup client builder by default values
        final KinesisAsyncClientBuilder kinesisAsyncClientBuilder = KinesisAsyncClient.builder().region( getRegion() );

        // Override endpoint if not one of the Prod stacks.
//        if (!isProd()) {
//            kinesisAsyncClientBuilder
//                    .endpointOverride(new URI(getEndpoint()));
//        }

        kinesisAsyncClientBuilder.httpClient( sdkAsyncHttpClient );


        if ( getProfile() != null ) {
            kinesisAsyncClientBuilder.credentialsProvider( ProfileCredentialsProvider.builder().profileName( getProfile() ).build() );
        } else {
            kinesisAsyncClientBuilder.credentialsProvider( DefaultCredentialsProvider.create() );
        }

        return kinesisAsyncClientBuilder.build();
    }

    default DynamoDbAsyncClient buildAsyncDynamoDbClient() throws IOException {
        final DynamoDbAsyncClientBuilder builder = DynamoDbAsyncClient.builder().region( getRegion() );

        if ( getProfile() != null ) {
            builder.credentialsProvider( ProfileCredentialsProvider.builder().profileName( getProfile() ).build() );
        } else {
            builder.credentialsProvider( DefaultCredentialsProvider.create() );
        }

        return builder.build();
    }

    default CloudWatchAsyncClient buildAsyncCloudWatchClient() throws IOException {
        final CloudWatchAsyncClientBuilder builder = CloudWatchAsyncClient.builder().region( getRegion() );

        if ( getProfile() != null ) {
            builder.credentialsProvider( ProfileCredentialsProvider.builder().profileName( getProfile() ).build() );
        } else {
            builder.credentialsProvider( DefaultCredentialsProvider.create() );
        }

        return builder.build();
    }

    default String getWorkerId() throws UnknownHostException {
        return Inet4Address.getLocalHost().getHostName();
    }

    default RecordValidatorQueue getRecordValidator() {
        return new RecordValidatorQueue();
    }

    default ShardRecordProcessorFactory getShardRecordProcessorFactory() {
        if (getKCLVersion() == KCLVersion.KCL2X) {
            return new TestRecordProcessorFactoryV2( getRecordValidator() );
        } else {
            return null;
        }
    }

    default ConfigsBuilder getConfigsBuilder() throws IOException, URISyntaxException {
        return getConfigsBuilder( "" );
    }

    default ConfigsBuilder getConfigsBuilder( String workerIdSuffix ) throws IOException, URISyntaxException {
        final String workerId = getWorkerId() + workerIdSuffix;
        if ( getStreamArn() == null ) {
            return new ConfigsBuilder( getStreamName(), getApplicationName(), buildConsumerClient(), buildAsyncDynamoDbClient(),
                    buildAsyncCloudWatchClient(), workerId, getShardRecordProcessorFactory() );
        } else {
            return new ConfigsBuilder( Arn.fromString( getStreamArn() ), getApplicationName(), buildConsumerClient(), buildAsyncDynamoDbClient(),
                    buildAsyncCloudWatchClient(), workerId, getShardRecordProcessorFactory() );
        }
    }

    RetrievalConfig getRetrievalConfig() throws IOException, URISyntaxException;

    @Data
    @Builder
    class ProducerConfig {
        private boolean isBatchPut;
        private int batchSize;
        private int recordSizeKB;
        private long callPeriodMills;
    }

    @Data
    @Builder
    class ReshardConfig {
        private Double[] reshardingFactorCycle;
        private int numReshardCycles;
        private long reshardFrequencyMillis;
    }

    @Data
    @Builder
    class MultiStreamRotatorConfig {
        private int totalStreams;
        private int maxStreamsToProcess;
        private long streamsRotationMillis;
    }

}
