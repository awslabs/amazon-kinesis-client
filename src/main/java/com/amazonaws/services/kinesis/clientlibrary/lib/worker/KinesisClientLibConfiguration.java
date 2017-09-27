/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License. 
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Date;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang.Validate;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.google.common.collect.ImmutableSet;

import lombok.Getter;

/**
 * Configuration for the Amazon Kinesis Client Library.
 */
public class KinesisClientLibConfiguration {

    private static final long EPSILON_MS = 25;

    /**
     * The location in the shard from which the KinesisClientLibrary will start fetching records from
     * when the application starts for the first time and there is no checkpoint for the shard.
     */
    public static final InitialPositionInStream DEFAULT_INITIAL_POSITION_IN_STREAM = InitialPositionInStream.LATEST;

    /**
     * Fail over time in milliseconds. A worker which does not renew it's lease within this time interval
     * will be regarded as having problems and it's shards will be assigned to other workers.
     * For applications that have a large number of shards, this may be set to a higher number to reduce
     * the number of DynamoDB IOPS required for tracking leases.
     */
    public static final long DEFAULT_FAILOVER_TIME_MILLIS = 10000L;

    /**
     * Max records to fetch from Kinesis in a single GetRecords call.
     */
    public static final int DEFAULT_MAX_RECORDS = 10000;

    /**
     * The default value for how long the {@link ShardConsumer} should sleep if no records are returned from the call to
     * {@link com.amazonaws.services.kinesis.AmazonKinesis#getRecords(com.amazonaws.services.kinesis.model.GetRecordsRequest)}.
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
     * Metrics level for which to enable CloudWatch metrics.
     */
    public static final MetricsLevel DEFAULT_METRICS_LEVEL = MetricsLevel.DETAILED;

    /**
     * Metrics dimensions that always will be enabled regardless of the config provided by user.
     */
    public static final Set<String> METRICS_ALWAYS_ENABLED_DIMENSIONS = ImmutableSet.of(
            MetricsHelper.OPERATION_DIMENSION_NAME);

    /**
     * Allowed dimensions for CloudWatch metrics. By default, worker ID dimension will be disabled.
     */
    public static final Set<String> DEFAULT_METRICS_ENABLED_DIMENSIONS = ImmutableSet.<String>builder().addAll(
            METRICS_ALWAYS_ENABLED_DIMENSIONS).add(MetricsHelper.SHARD_ID_DIMENSION_NAME).build();

    /**
     * Metrics dimensions that signify all possible dimensions.
     */
    public static final Set<String> METRICS_DIMENSIONS_ALL = ImmutableSet.of(IMetricsScope.METRICS_DIMENSIONS_ALL);

    /**
     * User agent set when Amazon Kinesis Client Library makes AWS requests.
     */
    public static final String KINESIS_CLIENT_LIB_USER_AGENT = "amazon-kinesis-client-library-java-1.8.5";

    /**
     * KCL will validate client provided sequence numbers with a call to Amazon Kinesis before checkpointing for calls
     * to {@link RecordProcessorCheckpointer#checkpoint(String)} by default.
     */
    public static final boolean DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING = true;

    /**
     * The max number of leases (shards) this worker should process.
     * This can be useful to avoid overloading (and thrashing) a worker when a host has resource constraints
     * or during deployment.
     * NOTE: Setting this to a low value can cause data loss if workers are not able to pick up all shards in the
     * stream due to the max limit.
     */
    public static final int DEFAULT_MAX_LEASES_FOR_WORKER = Integer.MAX_VALUE;

    /**
     * Max leases to steal from another worker at one time (for load balancing).
     * Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
     * but can cause higher churn in the system.
     */
    public static final int DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;

    /**
     * The Amazon DynamoDB table used for tracking leases will be provisioned with this read capacity.
     */
    public static final int DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY = 10;

    /**
     * The Amazon DynamoDB table used for tracking leases will be provisioned with this write capacity.
     */
    public static final int DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10;

    /**
     * The Worker will skip shard sync during initialization if there are one or more leases in the lease table. This
     * assumes that the shards and leases are in-sync. This enables customers to choose faster startup times (e.g.
     * during incremental deployments of an application).
     */
    public static final boolean DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST = false;

    /**
     * Default Shard prioritization strategy.
     */
    public static final ShardPrioritization DEFAULT_SHARD_PRIORITIZATION = new NoOpShardPrioritization();

    /**
     * The amount of milliseconds to wait before graceful shutdown forcefully terminates.
     */
    public static final long DEFAULT_SHUTDOWN_GRACE_MILLIS = 5000L;

    /**
     * The size of the thread pool to create for the lease renewer to use.
     */
    public static final int DEFAULT_MAX_LEASE_RENEWAL_THREADS = 20;

    private String applicationName;
    private String tableName;
    private String streamName;
    private String kinesisEndpoint;
    private String dynamoDBEndpoint;
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
    private MetricsLevel metricsLevel;
    private Set<String> metricsEnabledDimensions;
    private boolean validateSequenceNumberBeforeCheckpointing;
    private String regionName;
    private int maxLeasesForWorker;
    private int maxLeasesToStealAtOneTime;
    private int initialLeaseTableReadCapacity;
    private int initialLeaseTableWriteCapacity;
    private InitialPositionInStreamExtended initialPositionInStreamExtended;
    // This is useful for optimizing deployments to large fleets working on a stable stream.
    private boolean skipShardSyncAtWorkerInitializationIfLeasesExist;
    private ShardPrioritization shardPrioritization;
    private long shutdownGraceMillis;

    @Getter
    private Optional<Integer> timeoutInSeconds = Optional.empty();

    @Getter
    private Optional<Integer> retryGetRecordsInSeconds = Optional.empty();

    @Getter
    private Optional<Integer> maxGetRecordsThreadPool = Optional.empty();

    @Getter
    private int maxLeaseRenewalThreads = DEFAULT_MAX_LEASE_RENEWAL_THREADS;

    @Getter
    private RecordsFetcherFactory recordsFetcherFactory;

    /**
     * Constructor.
     *
     * @param applicationName Name of the Amazon Kinesis application.
     *        By default the application name is included in the user agent string used to make AWS requests. This
     *        can assist with troubleshooting (e.g. distinguish requests made by separate applications).
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
     *
     * @param applicationName Name of the Amazon Kinesis application
     *        By default the application name is included in the user agent string used to make AWS requests. This
     *        can assist with troubleshooting (e.g. distinguish requests made by separate applications).
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
        this(applicationName,
                streamName,
                null,
                null,
                DEFAULT_INITIAL_POSITION_IN_STREAM,
                kinesisCredentialsProvider,
                dynamoDBCredentialsProvider,
                cloudWatchCredentialsProvider,
                DEFAULT_FAILOVER_TIME_MILLIS,
                workerId,
                DEFAULT_MAX_RECORDS,
                DEFAULT_IDLETIME_BETWEEN_READS_MILLIS,
                DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST,
                DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
                DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
                DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
                new ClientConfiguration(),
                new ClientConfiguration(),
                new ClientConfiguration(),
                DEFAULT_TASK_BACKOFF_TIME_MILLIS,
                DEFAULT_METRICS_BUFFER_TIME_MILLIS,
                DEFAULT_METRICS_MAX_QUEUE_SIZE,
                DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING,
                null,
                DEFAULT_SHUTDOWN_GRACE_MILLIS);
    }

    /**
     * @param applicationName Name of the Kinesis application
     *        By default the application name is included in the user agent string used to make AWS requests. This
     *        can assist with troubleshooting (e.g. distinguish requests made by separate applications).
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
     * @param validateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
     *        with a call to Amazon Kinesis before checkpointing for calls to
     *        {@link RecordProcessorCheckpointer#checkpoint(String)}
     * @param regionName The region name for the service
     * @param shutdownGraceMillis The number of milliseconds before graceful shutdown terminates forcefully
     */
    // CHECKSTYLE:IGNORE HiddenFieldCheck FOR NEXT 26 LINES
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 26 LINES
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
                                         int metricsMaxQueueSize,
                                         boolean validateSequenceNumberBeforeCheckpointing,
                                         String regionName,
                                         long shutdownGraceMillis) {
        this(applicationName, streamName, kinesisEndpoint, null, initialPositionInStream, kinesisCredentialsProvider,
                dynamoDBCredentialsProvider, cloudWatchCredentialsProvider, failoverTimeMillis, workerId,
                maxRecords, idleTimeBetweenReadsInMillis,
                callProcessRecordsEvenForEmptyRecordList, parentShardPollIntervalMillis,
                shardSyncIntervalMillis, cleanupTerminatedShardsBeforeExpiry,
                kinesisClientConfig, dynamoDBClientConfig, cloudWatchClientConfig,
                taskBackoffTimeMillis, metricsBufferTimeMillis, metricsMaxQueueSize,
                validateSequenceNumberBeforeCheckpointing, regionName, shutdownGraceMillis);
    }

    /**
     * @param applicationName Name of the Kinesis application
     *        By default the application name is included in the user agent string used to make AWS requests. This
     *        can assist with troubleshooting (e.g. distinguish requests made by separate applications).
     * @param streamName Name of the Kinesis stream
     * @param kinesisEndpoint Kinesis endpoint
     * @param dynamoDBEndpoint DynamoDB endpoint
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
     * @param validateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
     *        with a call to Amazon Kinesis before checkpointing for calls to
     *        {@link RecordProcessorCheckpointer#checkpoint(String)}
     * @param regionName The region name for the service
     */
    // CHECKSTYLE:IGNORE HiddenFieldCheck FOR NEXT 26 LINES
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 26 LINES
    public KinesisClientLibConfiguration(String applicationName,
                                         String streamName,
                                         String kinesisEndpoint,
                                         String dynamoDBEndpoint,
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
                                         int metricsMaxQueueSize,
                                         boolean validateSequenceNumberBeforeCheckpointing,
                                         String regionName,
                                         long shutdownGraceMillis) {
        // Check following values are greater than zero
        checkIsValuePositive("FailoverTimeMillis", failoverTimeMillis);
        checkIsValuePositive("IdleTimeBetweenReadsInMillis", idleTimeBetweenReadsInMillis);
        checkIsValuePositive("ParentShardPollIntervalMillis", parentShardPollIntervalMillis);
        checkIsValuePositive("ShardSyncIntervalMillis", shardSyncIntervalMillis);
        checkIsValuePositive("MaxRecords", (long) maxRecords);
        checkIsValuePositive("TaskBackoffTimeMillis", taskBackoffTimeMillis);
        checkIsValuePositive("MetricsBufferTimeMills", metricsBufferTimeMillis);
        checkIsValuePositive("MetricsMaxQueueSize", (long) metricsMaxQueueSize);
        checkIsValuePositive("ShutdownGraceMillis", shutdownGraceMillis);
        checkIsRegionNameValid(regionName);
        this.applicationName = applicationName;
        this.tableName = applicationName;
        this.streamName = streamName;
        this.kinesisEndpoint = kinesisEndpoint;
        this.dynamoDBEndpoint = dynamoDBEndpoint;
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
        this.kinesisClientConfig = checkAndAppendKinesisClientLibUserAgent(kinesisClientConfig);
        this.dynamoDBClientConfig = checkAndAppendKinesisClientLibUserAgent(dynamoDBClientConfig);
        this.cloudWatchClientConfig = checkAndAppendKinesisClientLibUserAgent(cloudWatchClientConfig);
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
        this.metricsBufferTimeMillis = metricsBufferTimeMillis;
        this.metricsMaxQueueSize = metricsMaxQueueSize;
        this.metricsLevel = DEFAULT_METRICS_LEVEL;
        this.metricsEnabledDimensions = DEFAULT_METRICS_ENABLED_DIMENSIONS;
        this.validateSequenceNumberBeforeCheckpointing = validateSequenceNumberBeforeCheckpointing;
        this.regionName = regionName;
        this.maxLeasesForWorker = DEFAULT_MAX_LEASES_FOR_WORKER;
        this.maxLeasesToStealAtOneTime = DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME;
        this.initialLeaseTableReadCapacity = DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY;
        this.initialLeaseTableWriteCapacity = DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY;
        this.initialPositionInStreamExtended =
                InitialPositionInStreamExtended.newInitialPosition(initialPositionInStream);
        this.skipShardSyncAtWorkerInitializationIfLeasesExist = DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST;
        this.shardPrioritization = DEFAULT_SHARD_PRIORITIZATION;
        this.recordsFetcherFactory = new SimpleRecordsFetcherFactory(this.maxRecords);
    }

    /**
     * @param applicationName Name of the Kinesis application
     *        By default the application name is included in the user agent string used to make AWS requests. This
     *        can assist with troubleshooting (e.g. distinguish requests made by separate applications).
     * @param streamName Name of the Kinesis stream
     * @param kinesisEndpoint Kinesis endpoint
     * @param dynamoDBEndpoint DynamoDB endpoint
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
     * @param validateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
     *        with a call to Amazon Kinesis before checkpointing for calls to
     *        {@link RecordProcessorCheckpointer#checkpoint(String)}
     * @param regionName The region name for the service
     */
    // CHECKSTYLE:IGNORE HiddenFieldCheck FOR NEXT 26 LINES
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 26 LINES
    public KinesisClientLibConfiguration(String applicationName,
            String streamName,
            String kinesisEndpoint,
            String dynamoDBEndpoint,
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
            int metricsMaxQueueSize,
            boolean validateSequenceNumberBeforeCheckpointing,
            String regionName,
            RecordsFetcherFactory recordsFetcherFactory) {
        // Check following values are greater than zero
        checkIsValuePositive("FailoverTimeMillis", failoverTimeMillis);
        checkIsValuePositive("IdleTimeBetweenReadsInMillis", idleTimeBetweenReadsInMillis);
        checkIsValuePositive("ParentShardPollIntervalMillis", parentShardPollIntervalMillis);
        checkIsValuePositive("ShardSyncIntervalMillis", shardSyncIntervalMillis);
        checkIsValuePositive("MaxRecords", (long) maxRecords);
        checkIsValuePositive("TaskBackoffTimeMillis", taskBackoffTimeMillis);
        checkIsValuePositive("MetricsBufferTimeMills", metricsBufferTimeMillis);
        checkIsValuePositive("MetricsMaxQueueSize", (long) metricsMaxQueueSize);
        checkIsRegionNameValid(regionName);
        this.applicationName = applicationName;
        this.tableName = applicationName;
        this.streamName = streamName;
        this.kinesisEndpoint = kinesisEndpoint;
        this.dynamoDBEndpoint = dynamoDBEndpoint;
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
        this.kinesisClientConfig = checkAndAppendKinesisClientLibUserAgent(kinesisClientConfig);
        this.dynamoDBClientConfig = checkAndAppendKinesisClientLibUserAgent(dynamoDBClientConfig);
        this.cloudWatchClientConfig = checkAndAppendKinesisClientLibUserAgent(cloudWatchClientConfig);
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
        this.metricsBufferTimeMillis = metricsBufferTimeMillis;
        this.metricsMaxQueueSize = metricsMaxQueueSize;
        this.metricsLevel = DEFAULT_METRICS_LEVEL;
        this.metricsEnabledDimensions = DEFAULT_METRICS_ENABLED_DIMENSIONS;
        this.validateSequenceNumberBeforeCheckpointing = validateSequenceNumberBeforeCheckpointing;
        this.regionName = regionName;
        this.maxLeasesForWorker = DEFAULT_MAX_LEASES_FOR_WORKER;
        this.maxLeasesToStealAtOneTime = DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME;
        this.initialLeaseTableReadCapacity = DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY;
        this.initialLeaseTableWriteCapacity = DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY;
        this.initialPositionInStreamExtended =
                InitialPositionInStreamExtended.newInitialPosition(initialPositionInStream);
        this.skipShardSyncAtWorkerInitializationIfLeasesExist = DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST;
        this.shardPrioritization = DEFAULT_SHARD_PRIORITIZATION;
        this.recordsFetcherFactory = recordsFetcherFactory;
        this.shutdownGraceMillis = shutdownGraceMillis;
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

    private void checkIsRegionNameValid(String regionNameToCheck) {
        if (regionNameToCheck != null && RegionUtils.getRegion(regionNameToCheck) == null) {
            throw new IllegalArgumentException("The specified region name is not valid");
        }
    }

    /**
     * @return Name of the application
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * @return Name of the table to use in DynamoDB
     */
    public String getTableName() {
        return tableName;
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
    public boolean shouldCallProcessRecordsEvenForEmptyRecordList() {
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
     * @return DynamoDB endpoint
     */
    public String getDynamoDBEndpoint() {
        return dynamoDBEndpoint;
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
     * @return Metrics are buffered for at most this long before publishing.
     */
    public long getMetricsBufferTimeMillis() {
        return metricsBufferTimeMillis;
    }

    /**
     * @return Max number of metrics to buffer before publishing.
     */
    public int getMetricsMaxQueueSize() {
        return metricsMaxQueueSize;
    }

    /**
     * @return Metrics level enabled for metrics.
     */
    public MetricsLevel getMetricsLevel() {
        return metricsLevel;
    }

    /**
     * @return Enabled dimensions for metrics.
     */
    public Set<String> getMetricsEnabledDimensions() {
        // Unmodifiable set.
        return metricsEnabledDimensions;
    }

    /**
     * @return true if we should clean up leases of shards after processing is complete (don't wait for expiration)
     */
    public boolean shouldCleanupLeasesUponShardCompletion() {
        return cleanupLeasesUponShardCompletion;
    }

    /**
     * @return true if KCL should validate client provided sequence numbers with a call to Amazon Kinesis before
     *         checkpointing for calls to {@link RecordProcessorCheckpointer#checkpoint(String)}
     */
    public boolean shouldValidateSequenceNumberBeforeCheckpointing() {
        return validateSequenceNumberBeforeCheckpointing;
    }

    /**
     * @return Region for the service
     */
    public String getRegionName() {
        return regionName;
    }

    /**
     * @return true if Worker should skip syncing shards and leases at startup if leases are present
     */
    public boolean getSkipShardSyncAtWorkerInitializationIfLeasesExist() {
        return skipShardSyncAtWorkerInitializationIfLeasesExist;
    }

    /**
     * @return Max leases this Worker can handle at a time
     */
    public int getMaxLeasesForWorker() {
        return maxLeasesForWorker;
    }

    /**
     * @return Max leases to steal at one time (for load balancing)
     */
    public int getMaxLeasesToStealAtOneTime() {
        return maxLeasesToStealAtOneTime;
    }

    /**
     * @return Read capacity to provision when creating the lease table.
     */
    public int getInitialLeaseTableReadCapacity() {
        return initialLeaseTableReadCapacity;
    }

    /**
     * @return Write capacity to provision when creating the lease table.
     */
    public int getInitialLeaseTableWriteCapacity() {
        return initialLeaseTableWriteCapacity;
    }

    /**
     * Keeping it protected to forbid outside callers from depending on this internal object.
     * @return The initialPositionInStreamExtended object.
     */
    protected InitialPositionInStreamExtended getInitialPositionInStreamExtended() {
        return initialPositionInStreamExtended;
    }

    /**
     * @return The timestamp from where we need to start the application.
     * Valid only for initial position of type AT_TIMESTAMP, returns null for other positions.
     */
    public Date getTimestampAtInitialPositionInStream() {
        return initialPositionInStreamExtended.getTimestamp();
    }

    /**
     * @return Shard prioritization strategy.
     */
    public ShardPrioritization getShardPrioritizationStrategy() {
        return shardPrioritization;
    }

    /**
     * @return Graceful shutdown timeout
     */
    public long getShutdownGraceMillis() {
        return shutdownGraceMillis;
    }

    /*
    // CHECKSTYLE:IGNORE HiddenFieldCheck FOR NEXT 190 LINES
    /**
     * @param tableName name of the lease table in DynamoDB
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * @param kinesisEndpoint Kinesis endpoint
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withKinesisEndpoint(String kinesisEndpoint) {
        this.kinesisEndpoint = kinesisEndpoint;
        return this;
    }

    /**
     * @param dynamoDBEndpoint DynamoDB endpoint
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withDynamoDBEndpoint(String dynamoDBEndpoint) {
        this.dynamoDBEndpoint = dynamoDBEndpoint;
        return this;
    }

    /**
     * @param initialPositionInStream One of LATEST or TRIM_HORIZON. The Amazon Kinesis Client Library
     *        will start fetching records from this position when the application starts up if there are no checkpoints.
     *        If there are checkpoints, we will process records from the checkpoint position.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withInitialPositionInStream(InitialPositionInStream initialPositionInStream) {
        this.initialPositionInStream = initialPositionInStream;
        this.initialPositionInStreamExtended =
                InitialPositionInStreamExtended.newInitialPosition(initialPositionInStream);
        return this;
    }

    /**
     * @param timestamp The timestamp to use with the AT_TIMESTAMP value for initialPositionInStream.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withTimestampAtInitialPositionInStream(Date timestamp) {
        this.initialPositionInStream = InitialPositionInStream.AT_TIMESTAMP;
        this.initialPositionInStreamExtended = InitialPositionInStreamExtended.newInitialPositionAtTimestamp(timestamp);
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
     * Controls how long the KCL will sleep if no records are returned from Kinesis
     *
     * <p>
     * This value is only used when no records are returned; if records are returned, the {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ProcessTask} will
     * immediately retrieve the next set of records after the call to
     * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#processRecords(ProcessRecordsInput)}
     * has returned. Setting this value to high may result in the KCL being unable to catch up. If you are changing this
     * value it's recommended that you enable {@link #withCallProcessRecordsEvenForEmptyRecordList(boolean)}, and
     * monitor how far behind the records retrieved are by inspecting
     * {@link com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput#getMillisBehindLatest()}, and the
     * <a href=
     * "http://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html#kinesis-metrics-stream">CloudWatch
     * Metric: GetRecords.MillisBehindLatest</a>
     * </p>
     *
     * @param idleTimeBetweenReadsInMillis
     *            how long to sleep between GetRecords calls when no records are returned.
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
        ClientConfiguration tempClientConfig = checkAndAppendKinesisClientLibUserAgent(clientConfig);
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
        this.kinesisClientConfig = checkAndAppendKinesisClientLibUserAgent(kinesisClientConfig);
        return this;
    }

    /**
     * @param dynamoDBClientConfig Client configuration used by DynamoDB client
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withDynamoDBClientConfig(ClientConfiguration dynamoDBClientConfig) {
        this.dynamoDBClientConfig = checkAndAppendKinesisClientLibUserAgent(dynamoDBClientConfig);
        return this;
    }

    /**
     * @param cloudWatchClientConfig Client configuration used by CloudWatch client
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withCloudWatchClientConfig(ClientConfiguration cloudWatchClientConfig) {
        this.cloudWatchClientConfig = checkAndAppendKinesisClientLibUserAgent(cloudWatchClientConfig);
        return this;
    }

    /**
     * Override the default user agent (application name).
     *
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

    /**
     * @param metricsLevel Metrics level to enable.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withMetricsLevel(MetricsLevel metricsLevel) {
        this.metricsLevel = metricsLevel == null ? DEFAULT_METRICS_LEVEL : metricsLevel;
        return this;
    }

    /**
     * Sets metrics level that should be enabled. Possible values are:
     * NONE
     * SUMMARY
     * DETAILED
     *
     * @param metricsLevel Metrics level to enable.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withMetricsLevel(String metricsLevel) {
        this.metricsLevel = MetricsLevel.fromName(metricsLevel);
        return this;
    }

    /**
     * Sets the dimensions that are allowed to be emitted in metrics.
     * @param metricsEnabledDimensions Set of dimensions that are allowed.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withMetricsEnabledDimensions(Set<String> metricsEnabledDimensions) {
        if (metricsEnabledDimensions == null) {
            this.metricsEnabledDimensions = METRICS_ALWAYS_ENABLED_DIMENSIONS;
        } else if (metricsEnabledDimensions.contains(IMetricsScope.METRICS_DIMENSIONS_ALL)) {
            this.metricsEnabledDimensions = METRICS_DIMENSIONS_ALL;
        } else {
            this.metricsEnabledDimensions = ImmutableSet.<String>builder().addAll(
                    metricsEnabledDimensions).addAll(METRICS_ALWAYS_ENABLED_DIMENSIONS).build();
        }
        return this;
    }

    /**
     *
     * @param validateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
     *        with a call to Amazon Kinesis before checkpointing for calls to
     *        {@link RecordProcessorCheckpointer#checkpoint(String)}.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withValidateSequenceNumberBeforeCheckpointing(
            boolean validateSequenceNumberBeforeCheckpointing) {
        this.validateSequenceNumberBeforeCheckpointing = validateSequenceNumberBeforeCheckpointing;
        return this;
    }

    /**
     * If set to true, the Worker will not sync shards and leases during initialization if there are one or more leases
     * in the lease table. This assumes that the shards and leases are in-sync.
     * This enables customers to choose faster startup times (e.g. during incremental deployments of an application).
     *
     * @param skipShardSyncAtStartupIfLeasesExist Should Worker skip syncing shards and leases at startup (Worker
     *        initialization).
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withSkipShardSyncAtStartupIfLeasesExist(
            boolean skipShardSyncAtStartupIfLeasesExist) {
        this.skipShardSyncAtWorkerInitializationIfLeasesExist = skipShardSyncAtStartupIfLeasesExist;
        return this;
    }

    /**
     *
     * @param regionName The region name for the service
     * @return KinesisClientLibConfiguration
     */
    // CHECKSTYLE:IGNORE HiddenFieldCheck FOR NEXT 2 LINES
    public KinesisClientLibConfiguration withRegionName(String regionName) {
        checkIsRegionNameValid(regionName);
        this.regionName = regionName;
        return this;
    }

    /**
     * Worker will not acquire more than the specified max number of leases even if there are more
     * shards that need to be processed. This can be used in scenarios where a worker is resource constrained or
     * to prevent lease thrashing when small number of workers pick up all leases for small amount of time during
     * deployment.
     * Note that setting a low value may cause data loss (e.g. if there aren't enough Workers to make progress on all
     * shards). When setting the value for this property, one must ensure enough workers are present to process
     * shards and should consider future resharding, child shards that may be blocked on parent shards, some workers
     * becoming unhealthy, etc.
     *
     * @param maxLeasesForWorker Max leases this Worker can handle at a time
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withMaxLeasesForWorker(int maxLeasesForWorker) {
        checkIsValuePositive("maxLeasesForWorker", maxLeasesForWorker);
        this.maxLeasesForWorker = maxLeasesForWorker;
        return this;
    }

    /**
     * Max leases to steal from a more loaded Worker at one time (for load balancing).
     * Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
     * but can cause higher churn in the system.
     *
     * @param maxLeasesToStealAtOneTime Steal up to this many leases at one time (for load balancing)
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withMaxLeasesToStealAtOneTime(int maxLeasesToStealAtOneTime) {
        checkIsValuePositive("maxLeasesToStealAtOneTime", maxLeasesToStealAtOneTime);
        this.maxLeasesToStealAtOneTime = maxLeasesToStealAtOneTime;
        return this;
    }

    /**
     * @param initialLeaseTableReadCapacity Read capacity to provision when creating the lease table.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withInitialLeaseTableReadCapacity(int initialLeaseTableReadCapacity) {
        checkIsValuePositive("initialLeaseTableReadCapacity", initialLeaseTableReadCapacity);
        this.initialLeaseTableReadCapacity = initialLeaseTableReadCapacity;
        return this;
    }

    /**
     * @param initialLeaseTableWriteCapacity Write capacity to provision when creating the lease table.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withInitialLeaseTableWriteCapacity(int initialLeaseTableWriteCapacity) {
        checkIsValuePositive("initialLeaseTableWriteCapacity", initialLeaseTableWriteCapacity);
        this.initialLeaseTableWriteCapacity = initialLeaseTableWriteCapacity;
        return this;
    }

    /**
     * @param shardPrioritization Implementation of ShardPrioritization interface that should be used during processing.
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withShardPrioritizationStrategy(ShardPrioritization shardPrioritization) {
        if (shardPrioritization == null) {
            throw new IllegalArgumentException("shardPrioritization cannot be null");
        }
        this.shardPrioritization = shardPrioritization;
        return this;
    }

    /**
     * Sets the size of the thread pool that will be used to renew leases.
     *
     * Setting this to low may starve the lease renewal process, and cause the worker to lose leases at a higher rate.
     *
     * @param maxLeaseRenewalThreads
     *            the maximum size of the lease renewal thread pool
     * @throws IllegalArgumentException
     *             if maxLeaseRenewalThreads is <= 0
     * @return this configuration object
     */
    public KinesisClientLibConfiguration withMaxLeaseRenewalThreads(int maxLeaseRenewalThreads) {
        Validate.isTrue(maxLeaseRenewalThreads > 2,
                "The maximum number of lease renewal threads must be greater than or equal to 2.");
        this.maxLeaseRenewalThreads = maxLeaseRenewalThreads;

        return this;
    }


    /**
     * @param retryGetRecordsInSeconds the time in seconds to wait before the worker retries to get a record.
     * @return this configuration object.
     */
    public KinesisClientLibConfiguration withRetryGetRecordsInSeconds(final int retryGetRecordsInSeconds) {
        checkIsValuePositive("retryGetRecordsInSeconds", retryGetRecordsInSeconds);
        this.retryGetRecordsInSeconds = Optional.of(retryGetRecordsInSeconds);
        return this;
    }

    /**
     *@param maxGetRecordsThreadPool the max number of threads in the getRecords thread pool.
     *@return this configuration object
     */
    public KinesisClientLibConfiguration withMaxGetRecordsThreadPool(final int maxGetRecordsThreadPool) {
        checkIsValuePositive("maxGetRecordsThreadPool", maxGetRecordsThreadPool);
        this.maxGetRecordsThreadPool = Optional.of(maxGetRecordsThreadPool);
        return this;
    }

    /**
     *
     * @param maxCacheSize the max number of records stored in the getRecordsCache
     * @return this configuration object
     */
    public KinesisClientLibConfiguration withMaxCacheSize(final int maxCacheSize) {
        checkIsValuePositive("maxCacheSize", maxCacheSize);
        this.recordsFetcherFactory.setMaxSize(maxCacheSize);
        return this;
    }

    public KinesisClientLibConfiguration withMaxCacheByteSize(final int maxCacheByteSize) {
        checkIsValuePositive("maxCacheByteSize", maxCacheByteSize);
        this.recordsFetcherFactory.setMaxByteSize(maxCacheByteSize);
        return this;
    }

    public KinesisClientLibConfiguration withDataFetchingStrategy(String dataFetchingStrategy) {
        this.recordsFetcherFactory.setDataFetchingStrategy(DataFetchingStrategy.valueOf(dataFetchingStrategy.toUpperCase()));
        return this;
    }

    public KinesisClientLibConfiguration withMaxRecordsCount(final int maxRecordsCount) {
        checkIsValuePositive("maxRecordsCount", maxRecordsCount);
        this.recordsFetcherFactory.setMaxRecordsCount(maxRecordsCount);
        return this;
    }

    /**
     * @param timeoutInSeconds The timeout in seconds to wait for the MultiLangProtocol to wait for
     */
    public void withTimeoutInSeconds(final int timeoutInSeconds) {
        this.timeoutInSeconds = Optional.of(timeoutInSeconds);
    }

    /**
     * @param shutdownGraceMillis Time before gracefully shutdown forcefully terminates
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration withShutdownGraceMillis(long shutdownGraceMillis) {
        checkIsValuePositive("ShutdownGraceMillis", shutdownGraceMillis);
        this.shutdownGraceMillis = shutdownGraceMillis;
        return this;
    }

    /** 
     * @param idleMillisBetweenCalls Idle time between 2 getcalls from the data fetcher.
     * @return
     */
    public KinesisClientLibConfiguration withIdleMillisBetweenCalls(long idleMillisBetweenCalls) {
        checkIsValuePositive("IdleMillisBetweenCalls", idleMillisBetweenCalls);
        this.recordsFetcherFactory.setIdleMillisBetweenCalls(idleMillisBetweenCalls);
        return this;
    }
}
