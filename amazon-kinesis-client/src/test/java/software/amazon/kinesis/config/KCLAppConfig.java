package software.amazon.kinesis.config;

import lombok.Value;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.kinesis.utils.RecordValidatorQueue;
import software.amazon.kinesis.utils.ReshardOptions;
import software.amazon.kinesis.application.TestRecordProcessorFactory;
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
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * Default configuration for a producer or consumer used in integration tests.
 * Producer: puts records of size 60 KB at an interval of 100 ms
 * Consumer: streaming configuration (vs polling) that starts processing records at shard horizon
 */
@Slf4j
public abstract class KCLAppConfig {
    public static final String AWS_ACCOUNT_PROFILE_PROPERTY = "awsProfile";
    public static final String CROSS_ACCOUNT_PROFILE_PROPERTY = "awsCrossAccountProfile";
    public static final String CROSS_ACCOUNT_CONSUMER_NAME = "cross-account-consumer";
    public static final String INTEGRATION_TEST_RESOURCE_PREFIX = "KCLIntegrationTest";

    private String accountIdForConsumer = null;
    private String accountIdForStreamOwner = null;
    private List<String> streamNames = null;
    private KinesisAsyncClient kinesisAsyncClientForConsumer;
    private StsAsyncClient stsAsyncClientForConsumer;
    private KinesisAsyncClient kinesisAsyncClientForStreamOwner;
    private StsAsyncClient stsAsyncClientForStreamOwner;
    private DynamoDbAsyncClient dynamoDbAsyncClient;
    private CloudWatchAsyncClient cloudWatchAsyncClient;
    private RecordValidatorQueue recordValidator;

    /**
     * List of Strings, either stream names or valid stream Arns, to be used in testing. For single stream mode, return
     * a list of size 1. For multistream mode, return a list of size > 1.
     */
    public abstract List<Arn> getStreamArns();

    public List<String> getStreamNames() {
        if (this.streamNames == null) {
            return getStreamArns().stream().map(streamArn ->
                                          streamArn.toString().substring(streamArn.toString().indexOf("/") + 1))
                                  .collect(Collectors.toList());
        } else {
            return this.streamNames;
        }
    }

    public abstract String getTestName();

    public String getApplicationName() {
        return INTEGRATION_TEST_RESOURCE_PREFIX + getTestName();
    }

    public int getShardCount() { return 4; }

    public Region getRegion() { return Region.US_WEST_2; }

    /**
     * Gets credentials for passed in profile with "-DawsProfile" which should match "~/.aws/config". Otherwise,
     * uses default profile credentials chain.
     */
    private AwsCredentialsProvider getCredentialsProvider() {
        final String awsProfile = System.getProperty(AWS_ACCOUNT_PROFILE_PROPERTY);
        return (awsProfile != null) ?
                ProfileCredentialsProvider.builder().profileName(awsProfile).build() : DefaultCredentialsProvider.create();
    }

    public boolean isCrossAccount() {
        return false;
    }

    public AwsCredentialsProvider getCrossAccountCredentialsProvider() {
        return null;
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

    public List<ReshardOptions> getReshardFactorList() {
        return null;
    }

    public String getAccountIdForConsumer() {
        if (this.accountIdForConsumer == null) {
            try {
                this.accountIdForConsumer = FutureUtils.resolveOrCancelFuture(
                        buildStsAsyncClientForConsumer().getCallerIdentity(), Duration.ofSeconds(30)).account();
            }
            catch (Exception e) {
                log.error("Error when getting account ID through STS for consumer", e);
            }
        }
        return this.accountIdForConsumer;
    }

    public String getAccountIdForStreamOwner() {
        if (this.accountIdForStreamOwner == null) {
            try {
                this.accountIdForStreamOwner = FutureUtils.resolveOrCancelFuture(
                        buildStsAsyncClientForStreamOwner().getCallerIdentity(), Duration.ofSeconds(30)).account();
            }
            catch (Exception e) {
                log.error("Error when getting account ID through STS for consumer", e);
            }
        }
        return this.accountIdForStreamOwner;
    }

    public final KinesisAsyncClient buildAsyncKinesisClientForConsumer() throws URISyntaxException, IOException {
        if (this.kinesisAsyncClientForConsumer == null) {
            this.kinesisAsyncClientForConsumer = buildAsyncKinesisClient(getCredentialsProvider());
        }
        return this.kinesisAsyncClientForConsumer;
    }

    /**
     * Builds the kinesis client for the account which owns the Kinesis stream. For cross account, this can be a
     * different account than the account which gets records from the stream in the KCL.
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public final KinesisAsyncClient buildAsyncKinesisClientForStreamOwner() throws URISyntaxException, IOException {
        if (this.kinesisAsyncClientForStreamOwner == null) {
            final KinesisAsyncClient client;
            if (isCrossAccount()) {
                client = buildAsyncKinesisClient(getCrossAccountCredentialsProvider());
            } else {
                client = buildAsyncKinesisClient(getCredentialsProvider());
            }
            this.kinesisAsyncClientForStreamOwner = client;
        }
        return this.kinesisAsyncClientForStreamOwner;
    }


    private KinesisAsyncClient buildAsyncKinesisClient(AwsCredentialsProvider creds) throws URISyntaxException, IOException {
        // Setup H2 client config.
        final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(Integer.MAX_VALUE)
                .protocol(getKinesisClientProtocol());

        final SdkAsyncHttpClient sdkAsyncHttpClient =
                builder.buildWithDefaults(AttributeMap.builder().build());

        // Setup client builder by default values
        final KinesisAsyncClientBuilder kinesisAsyncClientBuilder = KinesisAsyncClient.builder().region(getRegion());
        kinesisAsyncClientBuilder.httpClient(sdkAsyncHttpClient);
        kinesisAsyncClientBuilder.credentialsProvider(creds);

        return kinesisAsyncClientBuilder.build();
    }

    private StsAsyncClient buildStsAsyncClientForConsumer() {
        if (this.stsAsyncClientForConsumer == null) {
            this.stsAsyncClientForConsumer = StsAsyncClient.builder()
                                                           .credentialsProvider(getCredentialsProvider())
                                                           .region(getRegion())
                                                           .build();
        }
        return this.stsAsyncClientForConsumer;
    }

    private StsAsyncClient buildStsAsyncClientForStreamOwner() {
        if (this.stsAsyncClientForStreamOwner == null) {
            final StsAsyncClient client;
            if (isCrossAccount()) {
                client = buildStsAsyncClient(getCrossAccountCredentialsProvider());
            }
            else {
                client = buildStsAsyncClient(getCredentialsProvider());
            }
            this.stsAsyncClientForStreamOwner = client;
        }
        return this.stsAsyncClientForStreamOwner;
    }

    private StsAsyncClient buildStsAsyncClient(AwsCredentialsProvider creds) {
        return StsAsyncClient.builder()
                .credentialsProvider(creds)
                .region(getRegion())
                .build();
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

    public final ConfigsBuilder getConfigsBuilder(Map<Arn, Arn> streamToConsumerArnsMap)
            throws IOException, URISyntaxException {
        final String workerId = getWorkerId();
        if (getStreamArns().size() == 1) {
            final SingleStreamTracker singleStreamTracker = new SingleStreamTracker(
                    StreamIdentifier.singleStreamInstance(getStreamArns().get(0)),
                    buildStreamConfigList(streamToConsumerArnsMap).get(0));
            return new ConfigsBuilder(singleStreamTracker, getApplicationName(),
                    buildAsyncKinesisClientForConsumer(), buildAsyncDynamoDbClient(), buildAsyncCloudWatchClient(), workerId,
                    getShardRecordProcessorFactory());
        } else {
            final MultiStreamTracker multiStreamTracker = new MultiStreamTracker() {
                @Override
                public List<StreamConfig> streamConfigList() {
                    return buildStreamConfigList(streamToConsumerArnsMap);
                }
                @Override
                public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
                    return new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();
                }
            };
            return new ConfigsBuilder(multiStreamTracker, getApplicationName(),
                    buildAsyncKinesisClientForConsumer(), buildAsyncDynamoDbClient(), buildAsyncCloudWatchClient(), workerId,
                    getShardRecordProcessorFactory());
        }
    }

    private List<StreamConfig> buildStreamConfigList(Map<Arn, Arn> streamToConsumerArnsMap) {
        return getStreamArns().stream().map(streamArn-> {
            final StreamIdentifier streamIdentifier;
            if (getStreamArns().size() == 1) {
                streamIdentifier = StreamIdentifier.singleStreamInstance(streamArn);
            } else { //is multi-stream
                streamIdentifier = StreamIdentifier.multiStreamInstance(streamArn, getCreationEpoch(streamArn));
            }

            if (streamToConsumerArnsMap != null) {
                final StreamConfig streamConfig = new StreamConfig(streamIdentifier,
                        InitialPositionInStreamExtended.newInitialPosition(getInitialPosition()));
                return streamConfig.consumerArn(streamToConsumerArnsMap.get(streamArn).toString());
            } else {
                return new StreamConfig(streamIdentifier, InitialPositionInStreamExtended.newInitialPosition(getInitialPosition()));
            }
        }).collect(Collectors.toList());
    }

    private long getCreationEpoch(Arn streamArn) {
        final DescribeStreamSummaryRequest request = DescribeStreamSummaryRequest.builder()
                .streamARN(streamArn.toString())
                .build();

       DescribeStreamSummaryResponse response = null;
        try {
            response = FutureUtils.resolveOrCancelFuture(
                    buildAsyncKinesisClientForStreamOwner().describeStreamSummary(request), Duration.ofSeconds(60));
        } catch (Exception e) {
            log.error("Exception when calling DescribeStreamSummary", e);
        }
        return response.streamDescriptionSummary().streamCreationTimestamp().toEpochMilli();
    }


    public abstract RetrievalMode getRetrievalMode();

    public RetrievalConfig getRetrievalConfig(ConfigsBuilder configsBuilder, Map<Arn, Arn> streamToConsumerArnsMap) {
        final RetrievalConfig config = configsBuilder.retrievalConfig();
        if (getRetrievalMode() == RetrievalMode.POLLING) {
                config.retrievalSpecificConfig(new PollingConfig(config.kinesisClient()));
        } else {
            if (getStreamArns().size() == 1) {
                final Arn consumerArn = streamToConsumerArnsMap.get(getStreamArns().get(0));
                config.retrievalSpecificConfig(new FanOutConfig(config.kinesisClient()).consumerArn(consumerArn.toString()));
            }
            // For CAA multi-stream EFO, consumerArn is specified in StreamConfig
        }
        return config;
    }

    public Arn buildStreamArn(String streamName) {
        final String partition = getRegion().metadata().partition().id();
        return Arn.fromString(String.join(":", "arn", partition, "kinesis", getRegion().id(),
                getAccountIdForStreamOwner(), "stream") + "/" + INTEGRATION_TEST_RESOURCE_PREFIX + streamName);
    }

    /**
     * Configure ingress load (batch size, record size, and calling interval)
     */
    @Value
    @Builder
    public static class ProducerConfig {
        private boolean isBatchPut;
        private int batchSize;
        private int recordSizeKB;
        private long callPeriodMills;
    }

}
