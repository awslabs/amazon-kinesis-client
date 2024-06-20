package com.amazonaws.services.kinesis.clientlibrary.config;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.utils.RecordValidatorQueue;
import lombok.Value;
import com.amazonaws.services.kinesis.clientlibrary.utils.ReshardOptions;

import lombok.Builder;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;

/**
 * Default configuration for a producer or consumer used in integration tests.
 * Producer: puts records of size 60 KB at an interval of 100 ms
 * Consumer: starts polling for records to process at shard horizon
 */
public abstract class KCLAppConfig {

    private AmazonKinesis kinesisClient;
    private AmazonDynamoDB dynamoDbClient;
    private AmazonCloudWatch cloudWatchClient;
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
    private AWSCredentialsProvider getCredentialsProvider() {
        String awsProfile = System.getProperty("awsProfile");
        return ((awsProfile != null) ?
                new com.amazonaws.auth.profile.ProfileCredentialsProvider(awsProfile) : new DefaultAWSCredentialsProviderChain());
    }

    public InitialPositionInStream getKclInitialPosition() {
        return InitialPositionInStream.TRIM_HORIZON;
    }

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

    public final AmazonKinesis buildSyncKinesisClient() throws IOException {

        if (this.kinesisClient == null) {
            AmazonKinesisClientBuilder builder;

            builder = AmazonKinesisClientBuilder.standard().withRegion(getRegion().id());
            builder = builder.withCredentials(getCredentialsProvider());
            this.kinesisClient = builder.build();
        }
        return this.kinesisClient;
    }

    public final AmazonDynamoDB buildSyncDynamoDbClient() throws IOException {
        if (this.dynamoDbClient == null) {
            AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClient.builder().withRegion(getRegion().id());
            builder = builder.withCredentials(getCredentialsProvider());
            this.dynamoDbClient = builder.build();
        }
        return this.dynamoDbClient;
    }

    public final AmazonCloudWatch buildSyncCloudWatchClient() throws IOException {
        if (this.cloudWatchClient == null) {
            AmazonCloudWatchClientBuilder builder = AmazonCloudWatchClient.builder().withRegion(getRegion().id());
            builder = builder.withCredentials(getCredentialsProvider());
            this.cloudWatchClient = builder.build();
        }
        return this.cloudWatchClient;
    }

    public final RecordValidatorQueue getRecordValidator() {
        if(this.recordValidator == null) {
            this.recordValidator = new RecordValidatorQueue();
        }
        return this.recordValidator;
    }

    public final String getWorkerId() throws UnknownHostException {
        return Inet4Address.getLocalHost().getHostName();
    }

    public final KinesisClientLibConfiguration getKclConfiguration() throws IOException {
        return new KinesisClientLibConfiguration(getStreamName(),
                getStreamName(),
                getCredentialsProvider(),
                getWorkerId()).withInitialPositionInStream(getKclInitialPosition());
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