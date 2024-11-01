/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.leases;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.LeaseCleanupConfig;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseManagementFactory;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.worker.metric.WorkerMetric;

/**
 * Used by the KCL to configure lease management.
 */
@Data
@Accessors(fluent = true)
public class LeaseManagementConfig {

    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(1);

    public static final long DEFAULT_LEASE_CLEANUP_INTERVAL_MILLIS =
            Duration.ofMinutes(1).toMillis();
    public static final long DEFAULT_COMPLETED_LEASE_CLEANUP_INTERVAL_MILLIS =
            Duration.ofMinutes(5).toMillis();
    public static final long DEFAULT_GARBAGE_LEASE_CLEANUP_INTERVAL_MILLIS =
            Duration.ofMinutes(30).toMillis();
    public static final long DEFAULT_PERIODIC_SHARD_SYNC_INTERVAL_MILLIS = 2 * 60 * 1000L;
    public static final boolean DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED = false;
    public static final boolean DEFAULT_LEASE_TABLE_PITR_ENABLED = false;
    public static final boolean DEFAULT_ENABLE_PRIORITY_LEASE_ASSIGNMENT = true;
    public static final int DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY = 3;

    public static final LeaseCleanupConfig DEFAULT_LEASE_CLEANUP_CONFIG = LeaseCleanupConfig.builder()
            .leaseCleanupIntervalMillis(DEFAULT_LEASE_CLEANUP_INTERVAL_MILLIS)
            .completedLeaseCleanupIntervalMillis(DEFAULT_COMPLETED_LEASE_CLEANUP_INTERVAL_MILLIS)
            .garbageLeaseCleanupIntervalMillis(DEFAULT_GARBAGE_LEASE_CLEANUP_INTERVAL_MILLIS)
            .build();

    /**
     * Name of the table to use in DynamoDB
     */
    @NonNull
    private final String tableName;
    /**
     * Client to be used to access DynamoDB service.
     */
    @NonNull
    private final DynamoDbAsyncClient dynamoDBClient;
    /**
     * Client to be used to access Kinesis Data Streams service.
     */
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    /**
     * Name of the Kinesis Data Stream to read records from.
     */
    @NonNull
    private String streamName;
    /**
     * Used to distinguish different workers/processes of a KCL application.
     */
    @NonNull
    private final String workerIdentifier;

    /**
     * Fail over time in milliseconds. A worker which does not renew it's lease within this time interval
     * will be regarded as having problems and it's shards will be assigned to other workers.
     * For applications that have a large number of shards, this may be set to a higher number to reduce
     * the number of DynamoDB IOPS required for tracking leases.
     *
     * <p>Default value: 10000L</p>
     */
    private long failoverTimeMillis = 10000L;

    /**
     * Whether workers should take very expired leases at priority. A very expired lease is when a worker does not
     * renew its lease in 3 * {@link LeaseManagementConfig#failoverTimeMillis}. Very expired leases will be taken at
     * priority for a worker which disregards the target leases for the worker but obeys
     * {@link LeaseManagementConfig#maxLeasesForWorker}. New leases for new shards due to shard mutation are
     * considered to be very expired and taken with priority.
     *
     * <p>Default value: true </p>
     */
    private boolean enablePriorityLeaseAssignment = DEFAULT_ENABLE_PRIORITY_LEASE_ASSIGNMENT;

    /**
     * Shard sync interval in milliseconds - e.g. wait for this long between shard sync tasks.
     *
     * <p>Default value: 60000L</p>
     */
    private long shardSyncIntervalMillis = 60000L;

    /**
     * Cleanup leases upon shards completion (don't wait until they expire in Kinesis).
     * Keeping leases takes some tracking/resources (e.g. they need to be renewed, assigned), so by default we try
     * to delete the ones we don't need any longer.
     *
     * <p>Default value: true</p>
     */
    private boolean cleanupLeasesUponShardCompletion = true;

    /**
     * Configuration for lease cleanup in {@link LeaseCleanupManager}.
     *
     * <p>Default lease cleanup interval value: 1 minute.</p>
     * <p>Default completed lease cleanup threshold: 5 minute.</p>
     * <p>Default garbage lease cleanup threshold: 30 minute.</p>
     */
    private final LeaseCleanupConfig leaseCleanupConfig = DEFAULT_LEASE_CLEANUP_CONFIG;

    /**
     * The max number of leases (shards) this worker should process.
     * This can be useful to avoid overloading (and thrashing) a worker when a host has resource constraints
     * or during deployment.
     *
     * <p>NOTE: Setting this to a low value can cause data loss if workers are not able to pick up all shards in the
     * stream due to the max limit.</p>
     *
     * <p>Default value: {@link Integer#MAX_VALUE}</p>
     */
    private int maxLeasesForWorker = Integer.MAX_VALUE;

    /**
     * Max leases to steal from another worker at one time (for load balancing).
     * Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
     * but can cause higher churn in the system.
     *
     * <p>Default value: 1</p>
     */
    private int maxLeasesToStealAtOneTime = 1;

    /**
     * The Amazon DynamoDB table used for tracking leases will be provisioned with this read capacity.
     *
     * <p>Default value: 10</p>
     */
    private int initialLeaseTableReadCapacity = 10;

    /**
     * The Amazon DynamoDB table used for tracking leases will be provisioned with this write capacity.
     *
     * <p>Default value: 10</p>
     */
    private int initialLeaseTableWriteCapacity = 10;

    /**
     * Configurable functional interface to override the existing shardDetector.
     */
    private Function<StreamConfig, ShardDetector> customShardDetectorProvider;

    /**
     * The size of the thread pool to create for the lease renewer to use.
     *
     * <p>Default value: 20</p>
     */
    private int maxLeaseRenewalThreads = 20;

    /**
     *
     */
    private boolean ignoreUnexpectedChildShards = false;

    /**
     *
     */
    private boolean consistentReads = false;

    private long listShardsBackoffTimeInMillis = 1500L;

    private int maxListShardsRetryAttempts = 50;

    public long epsilonMillis = 25L;

    private Duration dynamoDbRequestTimeout = DEFAULT_REQUEST_TIMEOUT;

    private BillingMode billingMode = BillingMode.PAY_PER_REQUEST;

    private WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig =
            new WorkerUtilizationAwareAssignmentConfig();

    /**
     * Whether to enable deletion protection on the DynamoDB lease table created by KCL. This does not update
     * already existing tables.
     *
     * <p>Default value: false
     */
    private boolean leaseTableDeletionProtectionEnabled = DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED;

    /**
     * Whether to enable PITR (point in time recovery) on the DynamoDB lease table created by KCL. If true, this can
     * update existing table's PITR.
     *
     * <p>Default value: false
     */
    private boolean leaseTablePitrEnabled = DEFAULT_LEASE_TABLE_PITR_ENABLED;

    /**
     * The list of tags to be applied to the DynamoDB table created for lease management.
     *
     * <p>Default value: {@link DefaultSdkAutoConstructList}
     */
    private Collection<Tag> tags = DefaultSdkAutoConstructList.getInstance();

    /**
     * Frequency (in millis) of the auditor job to scan for partial leases in the lease table.
     * If the auditor detects any hole in the leases for a stream, then it would trigger shard sync based on
     * {@link #leasesRecoveryAuditorInconsistencyConfidenceThreshold}
     */
    private long leasesRecoveryAuditorExecutionFrequencyMillis = DEFAULT_PERIODIC_SHARD_SYNC_INTERVAL_MILLIS;

    /**
     * Confidence threshold for the periodic auditor job to determine if leases for a stream in the lease table
     * is inconsistent. If the auditor finds same set of inconsistencies consecutively for a stream for this many times,
     * then it would trigger a shard sync.
     */
    private int leasesRecoveryAuditorInconsistencyConfidenceThreshold =
            DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY;

    /**
     * The initial position for getting records from Kinesis streams.
     *
     * <p>Default value: {@link InitialPositionInStream#TRIM_HORIZON}</p>
     */
    private InitialPositionInStreamExtended initialPositionInStream =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    private int maxCacheMissesBeforeReload = 1000;
    private long listShardsCacheAllowedAgeInSeconds = 30;
    private int cacheMissWarningModulus = 250;

    private MetricsFactory metricsFactory = new NullMetricsFactory();

    @Deprecated
    public LeaseManagementConfig(
            String tableName,
            DynamoDbAsyncClient dynamoDBClient,
            KinesisAsyncClient kinesisClient,
            String streamName,
            String workerIdentifier) {
        this.tableName = tableName;
        this.dynamoDBClient = dynamoDBClient;
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.workerIdentifier = workerIdentifier;
    }

    public LeaseManagementConfig(
            final String tableName,
            final String applicationName,
            final DynamoDbAsyncClient dynamoDBClient,
            final KinesisAsyncClient kinesisClient,
            final String workerIdentifier) {
        this.tableName = tableName;
        this.dynamoDBClient = dynamoDBClient;
        this.kinesisClient = kinesisClient;
        this.workerIdentifier = workerIdentifier;
        this.workerUtilizationAwareAssignmentConfig.workerMetricsTableConfig =
                new WorkerMetricsTableConfig(applicationName);
    }

    /**
     * Returns the metrics factory.
     *
     * <p>
     * NOTE: This method is deprecated and will be removed in a future release. This metrics factory is not being used
     * in the KCL.
     * </p>
     *
     * @return
     */
    @Deprecated
    public MetricsFactory metricsFactory() {
        return metricsFactory;
    }

    /**
     * Sets the metrics factory.
     *
     * <p>
     * NOTE: This method is deprecated and will be removed in a future release. This metrics factory is not being used
     * in the KCL.
     * </p>
     *
     * @param metricsFactory
     */
    @Deprecated
    public LeaseManagementConfig metricsFactory(final MetricsFactory metricsFactory) {
        this.metricsFactory = metricsFactory;
        return this;
    }

    /**
     * The {@link ExecutorService} to be used by {@link ShardSyncTaskManager}.
     *
     * <p>Default value: {@link LeaseManagementThreadPool}</p>
     */
    private ExecutorService executorService = new LeaseManagementThreadPool(new ThreadFactoryBuilder()
            .setNameFormat("ShardSyncTaskManager-%04d")
            .build());

    static class LeaseManagementThreadPool extends ThreadPoolExecutor {
        private static final long DEFAULT_KEEP_ALIVE_TIME = 60L;

        LeaseManagementThreadPool(ThreadFactory threadFactory) {
            super(
                    0,
                    Integer.MAX_VALUE,
                    DEFAULT_KEEP_ALIVE_TIME,
                    TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    threadFactory);
        }
    }

    /**
     * Callback used with DynamoDB lease management. Callback is invoked once the table is newly created and is in the
     * active status.
     *
     * <p>
     * Default value: {@link TableCreatorCallback#NOOP_TABLE_CREATOR_CALLBACK}
     * </p>
     */
    private TableCreatorCallback tableCreatorCallback = TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK;

    private HierarchicalShardSyncer hierarchicalShardSyncer;

    private LeaseManagementFactory leaseManagementFactory;

    public HierarchicalShardSyncer hierarchicalShardSyncer() {
        if (hierarchicalShardSyncer == null) {
            hierarchicalShardSyncer = new HierarchicalShardSyncer();
        }
        return hierarchicalShardSyncer;
    }

    /**
     * Configuration class for controlling the graceful handoff of leases.
     * This configuration allows tuning of the shutdown behavior during lease transfers.
     * <p>
     * It provides settings to control the timeout period for waiting on the record processor
     * to shut down and an option to enable or disable graceful lease handoff.
     * </p>
     */
    @Data
    @Builder
    @Accessors(fluent = true)
    public static class GracefulLeaseHandoffConfig {
        /**
         * The minimum amount of time (in milliseconds) to wait for the current shard's RecordProcessor
         * to gracefully shut down before forcefully transferring the lease to the next owner.
         * <p>
         * If each call to {@code processRecords} is expected to run longer than the default value,
         * it makes sense to set this to a higher value to ensure the RecordProcessor has enough
         * time to complete its processing.
         * </p>
         * <p>
         * Default value is 30,000 milliseconds (30 seconds).
         * </p>
         */
        @Builder.Default
        private long gracefulLeaseHandoffTimeoutMillis = 30_000L;
        /**
         * Flag to enable or disable the graceful lease handoff mechanism.
         * <p>
         * When set to {@code true}, the KCL will attempt to gracefully transfer leases by
         * allowing the shard's RecordProcessor sufficient time to complete processing before
         * handing off the lease to another worker. When {@code false}, the lease will be
         * handed off without waiting for the RecordProcessor to shut down gracefully. Note
         * that checkpointing is expected to be implemented inside {@code shutdownRequested}
         * for this feature to work end to end.
         * </p>
         * <p>
         * Default value is {@code true}.
         * </p>
         */
        @Builder.Default
        private boolean isGracefulLeaseHandoffEnabled = true;
    }

    private GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig =
            GracefulLeaseHandoffConfig.builder().build();

    @Deprecated
    public LeaseManagementFactory leaseManagementFactory() {
        if (leaseManagementFactory == null) {
            Validate.notEmpty(streamName(), "Stream name is empty");
            leaseManagementFactory = new DynamoDBLeaseManagementFactory(
                    kinesisClient(),
                    streamName(),
                    dynamoDBClient(),
                    tableName(),
                    workerIdentifier(),
                    executorService(),
                    initialPositionInStream(),
                    failoverTimeMillis(),
                    epsilonMillis(),
                    maxLeasesForWorker(),
                    maxLeasesToStealAtOneTime(),
                    maxLeaseRenewalThreads(),
                    cleanupLeasesUponShardCompletion(),
                    ignoreUnexpectedChildShards(),
                    shardSyncIntervalMillis(),
                    consistentReads(),
                    listShardsBackoffTimeInMillis(),
                    maxListShardsRetryAttempts(),
                    maxCacheMissesBeforeReload(),
                    listShardsCacheAllowedAgeInSeconds(),
                    cacheMissWarningModulus(),
                    initialLeaseTableReadCapacity(),
                    initialLeaseTableWriteCapacity(),
                    hierarchicalShardSyncer(),
                    tableCreatorCallback(),
                    dynamoDbRequestTimeout(),
                    billingMode(),
                    tags());
        }
        return leaseManagementFactory;
    }

    /**
     * Vends LeaseManagementFactory that performs serde based on leaseSerializer and shard sync based on isMultiStreamingMode
     * @param leaseSerializer
     * @param isMultiStreamingMode
     * @return LeaseManagementFactory
     */
    public LeaseManagementFactory leaseManagementFactory(
            final LeaseSerializer leaseSerializer, boolean isMultiStreamingMode) {
        if (leaseManagementFactory == null) {
            leaseManagementFactory = new DynamoDBLeaseManagementFactory(
                    kinesisClient(),
                    dynamoDBClient(),
                    tableName(),
                    workerIdentifier(),
                    executorService(),
                    failoverTimeMillis(),
                    enablePriorityLeaseAssignment(),
                    epsilonMillis(),
                    maxLeasesForWorker(),
                    maxLeasesToStealAtOneTime(),
                    maxLeaseRenewalThreads(),
                    cleanupLeasesUponShardCompletion(),
                    ignoreUnexpectedChildShards(),
                    shardSyncIntervalMillis(),
                    consistentReads(),
                    listShardsBackoffTimeInMillis(),
                    maxListShardsRetryAttempts(),
                    maxCacheMissesBeforeReload(),
                    listShardsCacheAllowedAgeInSeconds(),
                    cacheMissWarningModulus(),
                    initialLeaseTableReadCapacity(),
                    initialLeaseTableWriteCapacity(),
                    hierarchicalShardSyncer(),
                    tableCreatorCallback(),
                    dynamoDbRequestTimeout(),
                    billingMode(),
                    leaseTableDeletionProtectionEnabled(),
                    leaseTablePitrEnabled(),
                    tags(),
                    leaseSerializer,
                    customShardDetectorProvider(),
                    isMultiStreamingMode,
                    leaseCleanupConfig(),
                    workerUtilizationAwareAssignmentConfig(),
                    gracefulLeaseHandoffConfig);
        }
        return leaseManagementFactory;
    }

    /**
     * Set leaseManagementFactory and return the current LeaseManagementConfig instance.
     * @param leaseManagementFactory
     * @return LeaseManagementConfig
     */
    public LeaseManagementConfig leaseManagementFactory(final LeaseManagementFactory leaseManagementFactory) {
        this.leaseManagementFactory = leaseManagementFactory;
        return this;
    }

    @Data
    @Accessors(fluent = true)
    public static class WorkerUtilizationAwareAssignmentConfig {
        /**
         * This defines the frequency of capturing worker metric stats in memory. Default is 1s
         */
        private long inMemoryWorkerMetricsCaptureFrequencyMillis =
                Duration.ofSeconds(1L).toMillis();
        /**
         * This defines the frequency of reporting worker metric stats to storage. Default is 30s
         */
        private long workerMetricsReporterFreqInMillis = Duration.ofSeconds(30).toMillis();
        /**
         * These are the no. of metrics that are persisted in storage in WorkerMetricStats ddb table.
         */
        private int noOfPersistedMetricsPerWorkerMetrics = 10;
        /**
         * Option to disable workerMetrics to use in lease balancing.
         */
        private boolean disableWorkerMetrics = false;
        /**
         * List of workerMetrics for the application.
         */
        private List<WorkerMetric> workerMetricList = new ArrayList<>();
        /**
         * Max throughput per host KBps, default is unlimited.
         */
        private double maxThroughputPerHostKBps = Double.MAX_VALUE;
        /**
         * Percentage of value to achieve critical dampening during this case
         */
        private int dampeningPercentage = 60;
        /**
         * Percentage value used to trigger reBalance. If fleet has workers which are have metrics value more or less
         * than 10% of fleet level average then reBalance is triggered.
         * Leases are taken from workers with metrics value more than fleet level average. The load to take from these
         * workers is determined by evaluating how far they are with respect to fleet level average.
         */
        private int reBalanceThresholdPercentage = 10;

        /**
         * The allowThroughputOvershoot flag determines whether leases should still be taken even if
         * it causes the total assigned throughput to exceed the desired throughput to take for re-balance.
         * Enabling this flag provides more flexibility for the LeaseAssignmentManager to explore additional
         * assignment possibilities, which can lead to faster throughput convergence.
         */
        private boolean allowThroughputOvershoot = true;

        /**
         * Duration after which workerMetricStats entry from WorkerMetricStats table will be cleaned up. When an entry's
         * lastUpdateTime is older than staleWorkerMetricsEntryCleanupDuration from current time, entry will be removed
         * from the table.
         */
        private Duration staleWorkerMetricsEntryCleanupDuration = Duration.ofDays(1);

        /**
         * configuration to configure how to create the WorkerMetricStats table, such as table name,
         * billing mode, provisioned capacity. If no table name is specified, the table name will
         * default to applicationName-WorkerMetricStats. If no billing more is chosen, default is
         * On-Demand.
         */
        private WorkerMetricsTableConfig workerMetricsTableConfig;

        /**
         * Frequency to perform worker variance balancing. This value is used with respect to the LAM frequency,
         * that is every third (as default) iteration of LAM the worker variance balancing will be performed.
         * Setting it to 1 will make varianceBalancing run on every iteration of LAM and 2 on every 2nd iteration
         * and so on.
         * NOTE: LAM frequency = failoverTimeMillis
         */
        private int varianceBalancingFrequency = 3;

        /**
         * Alpha value used for calculating exponential moving average of worker's metricStats. Selecting
         * higher alpha value gives more weightage to recent value and thus low smoothing effect on computed average
         * and selecting smaller alpha values gives more weightage to past value and high smoothing effect.
         */
        private double workerMetricsEMAAlpha = 0.5;
    }

    public static class WorkerMetricsTableConfig extends DdbTableConfig {
        public WorkerMetricsTableConfig(final String applicationName) {
            super(applicationName, "WorkerMetricStats");
        }
    }
}
