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

package software.amazon.kinesis.leases.dynamodb;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.KinesisShardDetector;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseManagementFactory;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.metrics.MetricsFactory;

/**
 *
 */
@Data
@KinesisClientInternalApi
public class DynamoDBLeaseManagementFactory implements LeaseManagementFactory {

    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final DynamoDbAsyncClient dynamoDBClient;
    @NonNull
    private final String tableName;
    @NonNull
    private final String workerIdentifier;
    @NonNull
    private final ExecutorService executorService;
    @NonNull
    private final HierarchicalShardSyncer hierarchicalShardSyncer;
    @NonNull
    private final LeaseSerializer leaseSerializer;
    @NonNull
    private StreamConfig streamConfig;

    private Function<StreamConfig, ShardDetector> customShardDetectorProvider;

    private final long failoverTimeMillis;
    private final long epsilonMillis;
    private final int maxLeasesForWorker;
    private final int maxLeasesToStealAtOneTime;
    private final int maxLeaseRenewalThreads;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean ignoreUnexpectedChildShards;
    private final long shardSyncIntervalMillis;
    private final boolean consistentReads;
    private final long listShardsBackoffTimeMillis;
    private final int maxListShardsRetryAttempts;
    private final int maxCacheMissesBeforeReload;
    private final long listShardsCacheAllowedAgeInSeconds;
    private final int cacheMissWarningModulus;
    private final long initialLeaseTableReadCapacity;
    private final long initialLeaseTableWriteCapacity;
    private final TableCreatorCallback tableCreatorCallback;
    private final Duration dynamoDbRequestTimeout;
    private final BillingMode billingMode;

    /**
     * Constructor.
     *
     * <p>NOTE: This constructor is deprecated and will be removed in a future release.</p>
     *
     * @param kinesisClient
     * @param streamName
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param initialPositionInStream
     * @param failoverTimeMillis
     * @param epsilonMillis
     * @param maxLeasesForWorker
     * @param maxLeasesToStealAtOneTime
     * @param maxLeaseRenewalThreads
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIntervalMillis
     * @param consistentReads
     * @param listShardsBackoffTimeMillis
     * @param maxListShardsRetryAttempts
     * @param maxCacheMissesBeforeReload
     * @param listShardsCacheAllowedAgeInSeconds
     * @param cacheMissWarningModulus
     */
    @Deprecated
    public DynamoDBLeaseManagementFactory(final KinesisAsyncClient kinesisClient, final String streamName,
            final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String workerIdentifier,
            final ExecutorService executorService, final InitialPositionInStreamExtended initialPositionInStream,
            final long failoverTimeMillis, final long epsilonMillis, final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus) {
        this(kinesisClient, streamName, dynamoDBClient, tableName, workerIdentifier, executorService,
                initialPositionInStream, failoverTimeMillis, epsilonMillis, maxLeasesForWorker,
                maxLeasesToStealAtOneTime, maxLeaseRenewalThreads, cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards, shardSyncIntervalMillis, consistentReads, listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts, maxCacheMissesBeforeReload, listShardsCacheAllowedAgeInSeconds,
                cacheMissWarningModulus, TableConstants.DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY,
                TableConstants.DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY);
    }

    /**
     * Constructor.
     *
     * <p>
     * NOTE: This constructor is deprecated and will be removed in a future release.
     * </p>
     *
     * @param kinesisClient
     * @param streamName
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param initialPositionInStream
     * @param failoverTimeMillis
     * @param epsilonMillis
     * @param maxLeasesForWorker
     * @param maxLeasesToStealAtOneTime
     * @param maxLeaseRenewalThreads
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIntervalMillis
     * @param consistentReads
     * @param listShardsBackoffTimeMillis
     * @param maxListShardsRetryAttempts
     * @param maxCacheMissesBeforeReload
     * @param listShardsCacheAllowedAgeInSeconds
     * @param cacheMissWarningModulus
     * @param initialLeaseTableReadCapacity
     * @param initialLeaseTableWriteCapacity
     */
    @Deprecated
    public DynamoDBLeaseManagementFactory(final KinesisAsyncClient kinesisClient, final String streamName,
            final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String workerIdentifier,
            final ExecutorService executorService, final InitialPositionInStreamExtended initialPositionInStream,
            final long failoverTimeMillis, final long epsilonMillis, final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus,
            final long initialLeaseTableReadCapacity, final long initialLeaseTableWriteCapacity) {
        this(kinesisClient, streamName, dynamoDBClient, tableName, workerIdentifier, executorService,
                initialPositionInStream, failoverTimeMillis, epsilonMillis, maxLeasesForWorker,
                maxLeasesToStealAtOneTime, maxLeaseRenewalThreads, cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards, shardSyncIntervalMillis, consistentReads, listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts, maxCacheMissesBeforeReload, listShardsCacheAllowedAgeInSeconds,
                cacheMissWarningModulus, initialLeaseTableReadCapacity, initialLeaseTableWriteCapacity,
                new HierarchicalShardSyncer(), TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT);
    }

    /**
     * Constructor.
     *
     * @param kinesisClient
     * @param streamName
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param initialPositionInStream
     * @param failoverTimeMillis
     * @param epsilonMillis
     * @param maxLeasesForWorker
     * @param maxLeasesToStealAtOneTime
     * @param maxLeaseRenewalThreads
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIntervalMillis
     * @param consistentReads
     * @param listShardsBackoffTimeMillis
     * @param maxListShardsRetryAttempts
     * @param maxCacheMissesBeforeReload
     * @param listShardsCacheAllowedAgeInSeconds
     * @param cacheMissWarningModulus
     * @param initialLeaseTableReadCapacity
     * @param initialLeaseTableWriteCapacity
     * @param hierarchicalShardSyncer
     * @param tableCreatorCallback
     */
    @Deprecated
    public DynamoDBLeaseManagementFactory(final KinesisAsyncClient kinesisClient, final String streamName,
            final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String workerIdentifier,
            final ExecutorService executorService, final InitialPositionInStreamExtended initialPositionInStream,
            final long failoverTimeMillis, final long epsilonMillis, final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus,
            final long initialLeaseTableReadCapacity, final long initialLeaseTableWriteCapacity,
            final HierarchicalShardSyncer hierarchicalShardSyncer, final TableCreatorCallback tableCreatorCallback) {
        this(kinesisClient, streamName, dynamoDBClient, tableName, workerIdentifier, executorService,
                initialPositionInStream, failoverTimeMillis, epsilonMillis, maxLeasesForWorker,
                maxLeasesToStealAtOneTime, maxLeaseRenewalThreads, cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards, shardSyncIntervalMillis, consistentReads, listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts, maxCacheMissesBeforeReload, listShardsCacheAllowedAgeInSeconds,
                cacheMissWarningModulus, initialLeaseTableReadCapacity, initialLeaseTableWriteCapacity,
                hierarchicalShardSyncer, tableCreatorCallback, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT);
    }

    /**
     * Constructor.
     *
     * @param kinesisClient
     * @param streamName
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param initialPositionInStream
     * @param failoverTimeMillis
     * @param epsilonMillis
     * @param maxLeasesForWorker
     * @param maxLeasesToStealAtOneTime
     * @param maxLeaseRenewalThreads
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIntervalMillis
     * @param consistentReads
     * @param listShardsBackoffTimeMillis
     * @param maxListShardsRetryAttempts
     * @param maxCacheMissesBeforeReload
     * @param listShardsCacheAllowedAgeInSeconds
     * @param cacheMissWarningModulus
     * @param initialLeaseTableReadCapacity
     * @param initialLeaseTableWriteCapacity
     * @param hierarchicalShardSyncer
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     */
    @Deprecated
    public DynamoDBLeaseManagementFactory(final KinesisAsyncClient kinesisClient, final String streamName,
            final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String workerIdentifier,
            final ExecutorService executorService, final InitialPositionInStreamExtended initialPositionInStream,
            final long failoverTimeMillis, final long epsilonMillis, final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus,
            final long initialLeaseTableReadCapacity, final long initialLeaseTableWriteCapacity,
            final HierarchicalShardSyncer hierarchicalShardSyncer, final TableCreatorCallback tableCreatorCallback,
            Duration dynamoDbRequestTimeout) {
        this(kinesisClient, streamName, dynamoDBClient, tableName, workerIdentifier, executorService,
                initialPositionInStream, failoverTimeMillis, epsilonMillis, maxLeasesForWorker,
                maxLeasesToStealAtOneTime, maxLeaseRenewalThreads, cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards, shardSyncIntervalMillis, consistentReads, listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts, maxCacheMissesBeforeReload, listShardsCacheAllowedAgeInSeconds,
                cacheMissWarningModulus, initialLeaseTableReadCapacity, initialLeaseTableWriteCapacity,
                hierarchicalShardSyncer, tableCreatorCallback, dynamoDbRequestTimeout, BillingMode.PROVISIONED);
    }

    /**
     * Constructor.
     *
     * @param kinesisClient
     * @param streamName
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param initialPositionInStream
     * @param failoverTimeMillis
     * @param epsilonMillis
     * @param maxLeasesForWorker
     * @param maxLeasesToStealAtOneTime
     * @param maxLeaseRenewalThreads
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIntervalMillis
     * @param consistentReads
     * @param listShardsBackoffTimeMillis
     * @param maxListShardsRetryAttempts
     * @param maxCacheMissesBeforeReload
     * @param listShardsCacheAllowedAgeInSeconds
     * @param cacheMissWarningModulus
     * @param initialLeaseTableReadCapacity
     * @param initialLeaseTableWriteCapacity
     * @param hierarchicalShardSyncer
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     * @param billingMode
     */
    @Deprecated
    public DynamoDBLeaseManagementFactory(final KinesisAsyncClient kinesisClient, final String streamName,
            final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String workerIdentifier,
            final ExecutorService executorService, final InitialPositionInStreamExtended initialPositionInStream,
            final long failoverTimeMillis, final long epsilonMillis, final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus,
            final long initialLeaseTableReadCapacity, final long initialLeaseTableWriteCapacity,
            final HierarchicalShardSyncer hierarchicalShardSyncer, final TableCreatorCallback tableCreatorCallback,
            Duration dynamoDbRequestTimeout, BillingMode billingMode) {

        this(kinesisClient, new StreamConfig(StreamIdentifier.singleStreamInstance(streamName), initialPositionInStream), dynamoDBClient, tableName,
                workerIdentifier, executorService, failoverTimeMillis, epsilonMillis, maxLeasesForWorker,
                maxLeasesToStealAtOneTime, maxLeaseRenewalThreads, cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards, shardSyncIntervalMillis, consistentReads, listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts, maxCacheMissesBeforeReload, listShardsCacheAllowedAgeInSeconds,
                cacheMissWarningModulus, initialLeaseTableReadCapacity, initialLeaseTableWriteCapacity,
                hierarchicalShardSyncer, tableCreatorCallback, dynamoDbRequestTimeout, billingMode, new DynamoDBLeaseSerializer());
    }

    /**
     * Constructor.
     *
     * @param kinesisClient
     * @param streamConfig
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param failoverTimeMillis
     * @param epsilonMillis
     * @param maxLeasesForWorker
     * @param maxLeasesToStealAtOneTime
     * @param maxLeaseRenewalThreads
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIntervalMillis
     * @param consistentReads
     * @param listShardsBackoffTimeMillis
     * @param maxListShardsRetryAttempts
     * @param maxCacheMissesBeforeReload
     * @param listShardsCacheAllowedAgeInSeconds
     * @param cacheMissWarningModulus
     * @param initialLeaseTableReadCapacity
     * @param initialLeaseTableWriteCapacity
     * @param hierarchicalShardSyncer
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     * @param billingMode
     */
    private DynamoDBLeaseManagementFactory(final KinesisAsyncClient kinesisClient, final StreamConfig streamConfig,
            final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String workerIdentifier,
            final ExecutorService executorService, final long failoverTimeMillis, final long epsilonMillis,
            final int maxLeasesForWorker, final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus,
            final long initialLeaseTableReadCapacity, final long initialLeaseTableWriteCapacity,
            final HierarchicalShardSyncer hierarchicalShardSyncer, final TableCreatorCallback tableCreatorCallback,
            Duration dynamoDbRequestTimeout, BillingMode billingMode, LeaseSerializer leaseSerializer) {
        this(kinesisClient, dynamoDBClient, tableName,
                workerIdentifier, executorService, failoverTimeMillis, epsilonMillis, maxLeasesForWorker,
                maxLeasesToStealAtOneTime, maxLeaseRenewalThreads, cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards, shardSyncIntervalMillis, consistentReads, listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts, maxCacheMissesBeforeReload, listShardsCacheAllowedAgeInSeconds,
                cacheMissWarningModulus, initialLeaseTableReadCapacity, initialLeaseTableWriteCapacity,
                hierarchicalShardSyncer, tableCreatorCallback, dynamoDbRequestTimeout, billingMode, leaseSerializer,
                null);
        this.streamConfig = streamConfig;
    }

    /**
     * Constructor.
     * @param kinesisClient
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param failoverTimeMillis
     * @param epsilonMillis
     * @param maxLeasesForWorker
     * @param maxLeasesToStealAtOneTime
     * @param maxLeaseRenewalThreads
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIntervalMillis
     * @param consistentReads
     * @param listShardsBackoffTimeMillis
     * @param maxListShardsRetryAttempts
     * @param maxCacheMissesBeforeReload
     * @param listShardsCacheAllowedAgeInSeconds
     * @param cacheMissWarningModulus
     * @param initialLeaseTableReadCapacity
     * @param initialLeaseTableWriteCapacity
     * @param hierarchicalShardSyncer
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     * @param billingMode
     * @param leaseSerializer
     */
    public DynamoDBLeaseManagementFactory(final KinesisAsyncClient kinesisClient,
            final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String workerIdentifier,
            final ExecutorService executorService, final long failoverTimeMillis, final long epsilonMillis,
            final int maxLeasesForWorker, final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus,
            final long initialLeaseTableReadCapacity, final long initialLeaseTableWriteCapacity,
            final HierarchicalShardSyncer hierarchicalShardSyncer, final TableCreatorCallback tableCreatorCallback,
            Duration dynamoDbRequestTimeout, BillingMode billingMode, LeaseSerializer leaseSerializer,
            Function<StreamConfig, ShardDetector> customShardDetectorProvider) {
        this.kinesisClient = kinesisClient;
        this.dynamoDBClient = dynamoDBClient;
        this.tableName = tableName;
        this.workerIdentifier = workerIdentifier;
        this.executorService = executorService;
        this.failoverTimeMillis = failoverTimeMillis;
        this.epsilonMillis = epsilonMillis;
        this.maxLeasesForWorker = maxLeasesForWorker;
        this.maxLeasesToStealAtOneTime = maxLeasesToStealAtOneTime;
        this.maxLeaseRenewalThreads = maxLeaseRenewalThreads;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncIntervalMillis = shardSyncIntervalMillis;
        this.consistentReads = consistentReads;
        this.listShardsBackoffTimeMillis = listShardsBackoffTimeMillis;
        this.maxListShardsRetryAttempts = maxListShardsRetryAttempts;
        this.maxCacheMissesBeforeReload = maxCacheMissesBeforeReload;
        this.listShardsCacheAllowedAgeInSeconds = listShardsCacheAllowedAgeInSeconds;
        this.cacheMissWarningModulus = cacheMissWarningModulus;
        this.initialLeaseTableReadCapacity = initialLeaseTableReadCapacity;
        this.initialLeaseTableWriteCapacity = initialLeaseTableWriteCapacity;
        this.hierarchicalShardSyncer = hierarchicalShardSyncer;
        this.tableCreatorCallback = tableCreatorCallback;
        this.dynamoDbRequestTimeout = dynamoDbRequestTimeout;
        this.billingMode = billingMode;
        this.leaseSerializer = leaseSerializer;
        this.customShardDetectorProvider = customShardDetectorProvider;
    }

    @Override
    public LeaseCoordinator createLeaseCoordinator(@NonNull final MetricsFactory metricsFactory) {
        return new DynamoDBLeaseCoordinator(this.createLeaseRefresher(),
                workerIdentifier,
                failoverTimeMillis,
                epsilonMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime,
                maxLeaseRenewalThreads,
                initialLeaseTableReadCapacity,
                initialLeaseTableWriteCapacity,
                metricsFactory);
    }

    @Override @Deprecated
    public ShardSyncTaskManager createShardSyncTaskManager(@NonNull final MetricsFactory metricsFactory) {
        return new ShardSyncTaskManager(this.createShardDetector(),
                this.createLeaseRefresher(),
                streamConfig.initialPositionInStreamExtended(),
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardSyncIntervalMillis,
                executorService,
                hierarchicalShardSyncer,
                metricsFactory);
    }

    /**
     * Create ShardSyncTaskManager from the streamConfig passed
     * @param metricsFactory
     * @param streamConfig
     * @return ShardSyncTaskManager
     */
    @Override
    public ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory, StreamConfig streamConfig) {
        return new ShardSyncTaskManager(this.createShardDetector(streamConfig),
                this.createLeaseRefresher(),
                streamConfig.initialPositionInStreamExtended(),
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardSyncIntervalMillis,
                executorService,
                hierarchicalShardSyncer,
                metricsFactory);
    }

    @Override
    public DynamoDBLeaseRefresher createLeaseRefresher() {
        return new DynamoDBLeaseRefresher(tableName, dynamoDBClient, leaseSerializer, consistentReads,
                tableCreatorCallback, dynamoDbRequestTimeout, billingMode);
    }

    @Override
    @Deprecated
    public ShardDetector createShardDetector() {
        return new KinesisShardDetector(kinesisClient, streamConfig.streamIdentifier(),
                listShardsBackoffTimeMillis, maxListShardsRetryAttempts, listShardsCacheAllowedAgeInSeconds,
                maxCacheMissesBeforeReload, cacheMissWarningModulus, dynamoDbRequestTimeout);
    }

    /**
     * KinesisShardDetector supports reading from service only using streamName. Support for accountId and
     * stream creation epoch is yet to be provided.
     * @param streamConfig
     * @return ShardDetector
     */
    @Override
    public ShardDetector createShardDetector(StreamConfig streamConfig) {
        return customShardDetectorProvider != null ? customShardDetectorProvider.apply(streamConfig) :
                new KinesisShardDetector(kinesisClient, streamConfig.streamIdentifier(), listShardsBackoffTimeMillis,
                        maxListShardsRetryAttempts, listShardsCacheAllowedAgeInSeconds, maxCacheMissesBeforeReload,
                        cacheMissWarningModulus, dynamoDbRequestTimeout);
    }
}
