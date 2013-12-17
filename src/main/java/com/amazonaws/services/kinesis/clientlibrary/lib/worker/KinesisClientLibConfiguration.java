/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Configuration for the Amazon Kinesis Client Library.
 */
public class KinesisClientLibConfiguration {

    private static final long EPSILON_MS = 25;

    /**
     * Fail over time in milliseconds. A worker which does not renew it's lease within this time interval
     * will be regarded as having problems and it's shards will be assigned to other workers.
     * For applications that have a large number of shards, this msy be set to a higher number to reduce
     * the number of DynamoDB IOPS required for tracking leases.
     */
    public static final long DEFAULT_FAILOVER_TIME_MILLIS = 10000L;
    
    /**
     * Max records to fetch from Kinesis in a single GetRecords call.
     */
    public static final int DEFAULT_MAX_RECORDS = 10000;
    
    /**
     * Idle time between record reads in milliseconds.
     */
    public static final long DEFAULT_IDLETIME_BETWEEN_READS_MILLIS = 1000L;
    
    /**
     * Don't call processRecords() on the record processor for empty record lists.
     */
    public static final boolean DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST = false;
    
    /**
     * Interval in milliseconds between polling to check for parent shard completion.
     * Polling frequently will take up more DynamoDB IOPS (when there are leases for shards waiting on
     * completion of parent shards).
     */
    public static final long DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS = 10000L;
    
    /**
     * Shard sync interval in milliseconds - e.g. wait for this long between shard sync tasks.
     */
    public static final long DEFAULT_SHARD_SYNC_INTERVAL_MILLIS = 60000L;
    
    /**
     * Cleanup leases upon shards completion (don't wait until they expire in Kinesis).
     * Keeping leases takes some tracking/resources (e.g. they need to be renewed, assigned), so by default we try
     * to delete the ones we don't need any longer.
     */
    public static final boolean DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION = true;
    
    /**
     * Backoff time in milliseconds for Amazon Kinesis Client Library tasks (in the event of failures).
     */
    public static final long DEFAULT_TASK_BACKOFF_TIME_MILLIS = 500L;
    
    /**
     * Buffer metrics for at most this long before publishing to CloudWatch.
     */
    public static final long DEFAULT_METRICS_BUFFER_TIME_MILLIS = 10000L;
    
    /**
     * Buffer at most this many metrics before publishing to CloudWatch.
     */
    public static final int DEFAULT_METRICS_MAX_QUEUE_SIZE = 10000;

    /**
     * User agent set when Amazon Kinesis Client Library makes AWS requests.
     */
    public static final String KINESIS_CLIENT_LIB_USER_AGENT = "amazon-kinesis-client-library-java-1.0.0";

    private String applicationName;
    private String streamName;
    private String kinesisEndpoint;
    private InitialPositionInStream initialPositionInStream;
    private AWSCredentialsProvider kinesisCredentialsProvider;
    private AWSCredentialsProvider dynamoDBCredentialsProvider;
    private AWSCredentialsProvider cloudWatchCredentialsProvider;
    private long failoverTimeMillis;
    private String workerIdentifier;
    private long shardSyncIntervalMillis;
    private int maxRecords;
    private long idleTimeBetweenReadsInMillis;
    // Enables applications flush/checkpoint (if they have some data "in progress", but don't get new data for while)
    private boolean callProcessRecordsEvenForEmptyRecordList;
    private long parentShardPollIntervalMillis;
    private boolean cleanupLeasesUponShardCompletion;
    private ClientConfiguration kinesisClientConfig;
    private ClientConfiguration dynamoDBClientConfig;
    private ClientConfiguration cloudWatchClientConfig;
    private long taskBackoffTimeMillis;
    private long metricsBufferTimeMillis;
    private int metricsMaxQueueSize;

    /**
     * Constructor.
     * @param applicationName Name of the Amazon Kinesis application.
     *     By default the application name is included in the user agent string used to make AWS requests. This
     *     can assist with troubleshooting (e.g. distinguish requests made by separate applications).
     * @param streamName Name of the Kinesis stream
     * @param credentialsProvider Provides credentials used to sign AWS requests
     * @param workerId Used to distinguish different workers/processes of a Kinesis application
     */
    public KinesisClientLibConfiguration(String applicationName,
            String streamName,
            AWSCredentialsProvider credentialsProvider,
            String workerId) {
        this(applicationName, streamName, credentialsProvider, credentialsProvider, credentialsProvider, workerId);
    }

    /**
     * Constructor.
     * @param applicationName Name of the Amazon Kinesis application
     *     By default the application name is included in the user agent string used to make AWS requests. This
     *     can assist with troubleshooting (e.g. distinguish requests made by separate applications).
     * @param streamName Name of the Kinesis stream
     * @param kinesisCredentialsProvider Provides credentials used to access Kinesis
     * @param dynamoDBCredentialsProvider Provides credentials used to access DynamoDB
     * @param cloudWatchCredentialsProvider Provides credentials used to access CloudWatch
     * @param workerId Used to distinguish different workers/processes of a Kinesis application
     */
    public KinesisClientLibConfiguration(String applicationName,
            String streamName,
            AWSCredentialsProvider kinesisCredentialsProvider,
            AWSCredentialsProvider dynamoDBCredentialsProvider,
            AWSCredentialsProvider cloudWatchCredentialsProvider,
            String workerId) {
        this(applicationName, streamName, null, InitialPositionInStream.LATEST, kinesisCredentialsProvider,
                dynamoDBCredentialsProvider, cloudWatchCredentialsProvider, DEFAULT_FAILOVER_TIME_MILLIS, workerId,
                DEFAULT_MAX_RECORDS, DEFAULT_IDLETIME_BETWEEN_READS_MILLIS,
                DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST, DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
                DEFAULT_SHARD_SYNC_INTERVAL_MILLIS, DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
                new ClientConfiguration(), new ClientConfiguration(), new ClientConfiguration(),
                DEFAULT_TASK_BACKOFF_TIME_MILLIS, DEFAULT_METRICS_BUFFER_TIME_MILLIS,
                DEFAULT_METRICS_MAX_QUEUE_SIZE);
    }

    /**
     * @param applicationName Name of the Kinesis application
     *     By default the application name is included in the user agent string used to make AWS requests. This
     *     can assist with troubleshooting (e.g. distinguish requests made by separate applications).
     * @param streamName Name of the Kinesis stream
     * @param kinesisEndpoint Kinesis endpoint
     * @param initialPositionInStream One of LATEST or TRIM_HORIZON. The KinesisClientLibrary will start fetching
     *        records from that location in the stream when an application starts up for the first time and there 
     *        are no checkpoints. If there are checkpoints, then we start from the checkpoint position.
     * @param kinesisCredentialsProvider Provides credentials used to access Kinesis
     * @param dynamoDBCredentialsProvider Provides credentials used to access DynamoDB
     * @param cloudWatchCredentialsProvider Provides credentials used to access CloudWatch
     * @param failoverTimeMillis Lease duration (leases not renewed within this period will be claimed by others)
     * @param workerId Used to distinguish different workers/processes of a Kinesis application
     * @param maxRecords Max records to read per Kinesis getRecords() call
     * @param idleTimeBetweenReadsInMillis Idle time between calls to fetch data from Kinesis
     * @param callProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
     *        GetRecords returned an empty record list.
     * @param parentShardPollIntervalMillis Wait for this long between polls to check if parent shards are done
     * @param shardSyncIntervalMillis Time between tasks to sync leases and Kinesis shards
     * @param cleanupTerminatedShardsBeforeExpiry Clean up shards we've finished processing (don't wait for expiration
     *        in Kinesis)
     * @param kinesisClientConfig Client Configuration used by Kinesis client
     * @param dynamoDBClientConfig Client Configuration used by DynamoDB client
     * @param cloudWatchClientConfig Client Configuration used by CloudWatch client
     * @param taskBackoffTimeMillis Backoff period when tasks encounter an exception
     * @param metricsBufferTimeMillis Metrics are buffered for at most this long before publishing to CloudWatch
     * @param metricsMaxQueueSize Max number of metrics to buffer before publishing to CloudWatch
     * 
     */
    // CHECKSTYLE:IGNORE HiddenFieldCheck FOR NEXT 25 LINES
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 25 LINES
    public KinesisClientLibConfiguration(String applicationName,
            String streamName,
            String kinesisEndpoint,
            InitialPositionInStream initialPositionInStream,
            AWSCredentialsProvider kinesisCredentialsProvider,
            AWSCredentialsProvider dynamoDBCredentialsProvider,
            AWSCredentialsProvider cloudWatchCredentialsProvider,
            long failoverTimeMillis,
            String workerId,
            int maxRecords,
            long idleTimeBetweenReadsInMillis,
            boolean callProcessRecordsEvenForEmptyRecordList,
            long parentShardPollIntervalMillis,
            long shardSyncIntervalMillis,
            boolean cleanupTerminatedShardsBeforeExpiry,
            ClientConfiguration kinesisClientConfig,
            ClientConfiguration dynamoDBClientConfig,
            ClientConfiguration cloudWatchClientConfig,
            long taskBackoffTimeMillis,
            long metricsBufferTimeMillis,
            int metricsMaxQueueSize) {
        // Check following values are greater than zero
        checkIsValuePositive("FailoverTimeMillis", failoverTimeMillis);
        checkIsValuePositive("IdleTimeBetweenReadsInMillis", idleTimeBetweenReadsInMillis);
        checkIsValuePositive("ParentShardPollIntervalMillis", parentShardPollIntervalMillis);
        checkIsValuePositive("ShardSyncIntervalMillis", shardSyncIntervalMillis);
        checkIsValuePositive("MaxRecords", (long) maxRecords);
        checkIsValuePositive("TaskBackoffTimeMillis", taskBackoffTimeMillis);
        checkIsValuePositive("MetricsBufferTimeMills", metricsBufferTimeMillis);
        checkIsValuePositive("MetricsMaxQueueSize", (long) metricsMaxQueueSize);
        this.applicationName = applicationName;
        this.streamName = streamName;
        this.kinesisEndpoint = kinesisEndpoint;
        this.initialPositionInStream = initialPositionInStream;
        this.kinesisCredentialsProvider = kinesisCredentialsProvider;
        this.dynamoDBCredentialsProvider = dynamoDBCredentialsProvider;
        this.cloudWatchCredentialsProvider = cloudWatchCredentialsProvider;
        this.failoverTimeMillis = failoverTimeMillis;
        this.maxRecords = maxRecords;
        this.idleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis;
        this.callProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.shardSyncIntervalMillis = shardSyncIntervalMillis;
        this.cleanupLeasesUponShardCompletion = cleanupTerminatedShardsBeforeExpiry;
        this.workerIdentifier = workerId;
        this.kinesisClientConfig = 
                checkAndAppendKinesisClientLibUserAgent(kinesisClientConfig);
        this.dynamoDBClientConfig = 
                checkAndAppendKinesisClientLibUserAgent(dynamoDBClientConfig);
        this.cloudWatchClientConfig = 
                checkAndAppendKinesisClientLibUserAgent(cloudWatchClientConfig);
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
        this.metricsBufferTimeMillis = metricsBufferTimeMillis;
        this.metricsMaxQueueSize = metricsMaxQueueSize;
    }

    // Check if value is positive, otherwise throw an exception
    private void checkIsValuePositive(String key, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Value of " + key 
                    + " should be positive, but current value is " + value);
        }
    }
    
    // Check if user agent in configuration is the default agent.
    // If so, replace it with application name plus KINESIS_CLIENT_LIB_USER_AGENT.
    // If not, append KINESIS_CLIENT_LIB_USER_AGENT to the end.
    private ClientConfiguration checkAndAppendKinesisClientLibUserAgent(ClientConfiguration config) {
        String existingUserAgent = config.getUserAgent();
        if (existingUserAgent.equals(ClientConfiguration.DEFAULT_USER_AGENT)) {
            existingUserAgent = applicationName;
        }
        if (!existingUserAgent.contains(KINESIS_CLIENT_LIB_USER_AGENT)) {
            existingUserAgent += "," + KINESIS_CLIENT_LIB_USER_AGENT;
        }
        config.setUserAgent(existingUserAgent);
        return config;
    }

    /**
     * @return Name of the application
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * @return Time within which a worker should renew a lease (else it is assumed dead)
     */
    public long getFailoverTimeMillis() {
        return failoverTimeMillis;
    }

    /**
     * @return Credentials provider used to access Kinesis
     */
    public AWSCredentialsProvider getKinesisCredentialsProvider() {
        return kinesisCredentialsProvider;
    }

    /**
     * @return Credentials provider used to access DynamoDB
     */
    public AWSCredentialsProvider getDynamoDBCredentialsProvider() {
        return dynamoDBCredentialsProvider;
    }

    /**
     * @return Credentials provider used to access CloudWatch
     */
    public AWSCredentialsProvider getCloudWatchCredentialsProvider() {
        return cloudWatchCredentialsProvider;
    }

    /**
     * @return workerIdentifier
     */
    public String getWorkerIdentifier() {
        return workerIdentifier;
    }

    /**
     * @return the shardSyncIntervalMillis
     */
    public long getShardSyncIntervalMillis() {
        return shardSyncIntervalMillis;
    }

    /**
     * @return Max records to fetch per Kinesis getRecords call
     */
    public int getMaxRecords() {
        return maxRecords;
    }

    /**
     * @return Idle time between calls to fetch data from Kinesis
     */
    public long getIdleTimeBetweenReadsInMillis() {
        return idleTimeBetweenReadsInMillis;
    }

    /**
     * @return true if processRecords() should be called even for empty record lists
     */
    boolean shouldCallProcessRecordsEvenForEmptyRecordList() {
        return callProcessRecordsEvenForEmptyRecordList;
    }

    /**
     * @return Epsilon milliseconds (used for lease timing margins)
     */
    public long getEpsilonMillis() {
        return EPSILON_MS;
    }

    /**
     * @return stream name
     */
    public String getStreamName() {
        return streamName;
    }

    /**
     * @return Kinesis endpoint
     */
    public String getKinesisEndpoint() {
        return kinesisEndpoint;
    }

    /**
     * @return the initialPositionInStream
     */
    public InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }

    /**
     * @return interval between polls for parent shard completion
     */
    public long getParentShardPollIntervalMillis() {
        return parentShardPollIntervalMillis;
    }

    /**
     * @return Kinesis client configuration 
     */
    public ClientConfiguration getKinesisClientConfiguration() {
        return kinesisClientConfig;
    }
    
    /**
     * @return DynamoDB client configuration
     */
    public ClientConfiguration getDynamoDBClientConfiguration() {
        return dynamoDBClientConfig;
    }
    
    /**
     * @return CloudWatch client configuration
     */
    public ClientConfiguration getCloudWatchClientConfiguration() {
        return cloudWatchClientConfig;
    }

    /**
     * @return backoff time when tasks encounter exceptions
     */
    public long getTaskBackoffTimeMillis() {
        return taskBackoffTimeMillis;
    }

    /**
     * @return Metrics are buffered for at most this long before publishing to CloudWatch
     */
    public long getMetricsBufferTimeMillis() {
        return metricsBufferTimeMillis;
    }

    /**
     * @return Max number of metrics to buffer before publishing to CloudWatch
     */
    public int getMetricsMaxQueueSize() {
        return metricsMaxQueueSize;
    }

    /**
     * @return true if we should clean up leases of shards after processing is complete (don't wait for expiration)
     */
    public boolean shouldCleanupLeasesUponShardCompletion() {
        return cleanupLeasesUponShardCompletion;
    }

    // CHECKSTYLE:IGNORE HiddenFieldCheck FOR NEXT 180 LINES
    /**
     * @param kinesisEndpoint Kinesis endpoint
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withKinesisEndpoint(String kinesisEndpoint) {
        this.kinesisEndpoint = kinesisEndpoint;
        return this;
    }

    /**
     * @param initialPositionInStream One of LATEST or TRIM_HORIZON. The Amazon Kinesis Client Library will start
     *        fetching records from this position when the application starts up if there are no checkpoints. If there
     *        are checkpoints, we will process records from the checkpoint position.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withInitialPositionInStream(InitialPositionInStream initialPositionInStream) {
        this.initialPositionInStream = initialPositionInStream;
        return this;
    }

    /**
     * @param failoverTimeMillis Lease duration (leases not renewed within this period will be claimed by others)
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withFailoverTimeMillis(long failoverTimeMillis) {
        checkIsValuePositive("FailoverTimeMillis", failoverTimeMillis);
        this.failoverTimeMillis = failoverTimeMillis;
        return this;
    }

    /**
     * @param shardSyncIntervalMillis Time between tasks to sync leases and Kinesis shards
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withShardSyncIntervalMillis(long shardSyncIntervalMillis) {
        checkIsValuePositive("ShardSyncIntervalMillis", shardSyncIntervalMillis);
        this.shardSyncIntervalMillis = shardSyncIntervalMillis;
        return this;
    }

    /**
     * @param maxRecords Max records to fetch in a Kinesis getRecords() call
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withMaxRecords(int maxRecords) {
        checkIsValuePositive("MaxRecords", (long) maxRecords);
        this.maxRecords = maxRecords;
        return this;
    }

    /**
     * @param idleTimeBetweenReadsInMillis Idle time between calls to fetch data from Kinesis
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withIdleTimeBetweenReadsInMillis(long idleTimeBetweenReadsInMillis) {
        checkIsValuePositive("IdleTimeBetweenReadsInMillis", idleTimeBetweenReadsInMillis);
        this.idleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis;
        return this;
    }

    /**
     * @param callProcessRecordsEvenForEmptyRecordList Call the RecordProcessor::processRecords() API even if
     *        GetRecords returned an empty record list
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withCallProcessRecordsEvenForEmptyRecordList(
        boolean callProcessRecordsEvenForEmptyRecordList) {
        this.callProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList;
        return this;
    }

    /**
     * @param parentShardPollIntervalMillis Wait for this long between polls to check if parent shards are done
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withParentShardPollIntervalMillis(long parentShardPollIntervalMillis) {
        checkIsValuePositive("ParentShardPollIntervalMillis", parentShardPollIntervalMillis);
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        return this;
    }

    /**
     * @param cleanupLeasesUponShardCompletion Clean up shards we've finished processing (don't wait for expiration
     *        in Kinesis)
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withCleanupLeasesUponShardCompletion(
        boolean cleanupLeasesUponShardCompletion) {
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        return this;
    }
    
    /**
     * @param clientConfig Common client configuration used by Kinesis/DynamoDB/CloudWatch client
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withCommonClientConfig(ClientConfiguration clientConfig) {
        ClientConfiguration tempClientConfig = 
                checkAndAppendKinesisClientLibUserAgent(clientConfig);
        this.kinesisClientConfig = tempClientConfig;
        this.dynamoDBClientConfig = tempClientConfig;
        this.cloudWatchClientConfig = tempClientConfig;
        return this;
    }

    /**
     * @param kinesisClientConfig Client configuration used by Kinesis client
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withKinesisClientConfig(ClientConfiguration kinesisClientConfig) {
        this.kinesisClientConfig = 
                checkAndAppendKinesisClientLibUserAgent(kinesisClientConfig);
        return this;
    }
    
    /**
     * @param dynamoDBClientConfig Client configuration used by DynamoDB client
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withDynamoDBClientConfig(ClientConfiguration dynamoDBClientConfig) {
        this.dynamoDBClientConfig = 
                checkAndAppendKinesisClientLibUserAgent(dynamoDBClientConfig);
        return this;
    }
    
    /**
     * @param cloudWatchClientConfig Client configuration used by CloudWatch client
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withCloudWatchClientConfig(ClientConfiguration cloudWatchClientConfig) {
        this.cloudWatchClientConfig = 
                checkAndAppendKinesisClientLibUserAgent(cloudWatchClientConfig);
        return this;
    }
    
    /**
     * Override the default user agent (application name).
     * @param userAgent User agent to use in AWS requests
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withUserAgent(String userAgent) {
        String customizedUserAgent = userAgent + "," + KINESIS_CLIENT_LIB_USER_AGENT;
        this.kinesisClientConfig.setUserAgent(customizedUserAgent);
        this.dynamoDBClientConfig.setUserAgent(customizedUserAgent);
        this.cloudWatchClientConfig.setUserAgent(customizedUserAgent);
        return this;
    }

    /**
     * @param taskBackoffTimeMillis Backoff period when tasks encounter an exception
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withTaskBackoffTimeMillis(long taskBackoffTimeMillis) {
        checkIsValuePositive("TaskBackoffTimeMillis", taskBackoffTimeMillis);
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
        return this;
    }

    /**
     * @param metricsBufferTimeMillis Metrics are buffered for at most this long before publishing to CloudWatch
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withMetricsBufferTimeMillis(long metricsBufferTimeMillis) {
        checkIsValuePositive("MetricsBufferTimeMillis", metricsBufferTimeMillis);
        this.metricsBufferTimeMillis = metricsBufferTimeMillis;
        return this;
    }

    /**
     * @param metricsMaxQueueSize Max number of metrics to buffer before publishing to CloudWatch
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withMetricsMaxQueueSize(int metricsMaxQueueSize) {
        checkIsValuePositive("MetricsMaxQueueSize", (long) metricsMaxQueueSize);
        this.metricsMaxQueueSize = metricsMaxQueueSize;
        return this;
    }
}
