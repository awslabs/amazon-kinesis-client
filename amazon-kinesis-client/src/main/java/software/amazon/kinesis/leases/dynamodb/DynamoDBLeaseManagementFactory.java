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
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.LeaseCleanupConfig;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.DeletedStreamListProvider;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.KinesisShardDetector;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseManagementFactory;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.metrics.MetricsFactory;

/**
 *
 */
@Slf4j
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
    private final HierarchicalShardSyncer deprecatedHierarchicalShardSyncer;

    @NonNull
    private final LeaseSerializer leaseSerializer;

    private final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig;

    @NonNull
    private StreamConfig streamConfig;

    private Function<StreamConfig, ShardDetector> customShardDetectorProvider;

    private final long failoverTimeMillis;
    private final boolean enablePriorityLeaseAssignment;
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
    private final boolean leaseTableDeletionProtectionEnabled;
    private final boolean leaseTablePitrEnabled;
    private final Collection<Tag> tags;
    private final boolean isMultiStreamMode;
    private final LeaseCleanupConfig leaseCleanupConfig;
    private final LeaseManagementConfig.GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig;

    /**
     * Constructor.
     * @deprecated this is used by the deprecated method in LeaseManagementConfig to construct the LeaseManagement factory
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
     * @param tags
     */
    @Deprecated
    public DynamoDBLeaseManagementFactory(
            final KinesisAsyncClient kinesisClient,
            final String streamName,
            final DynamoDbAsyncClient dynamoDBClient,
            final String tableName,
            final String workerIdentifier,
            final ExecutorService executorService,
            final InitialPositionInStreamExtended initialPositionInStream,
            final long failoverTimeMillis,
            final long epsilonMillis,
            final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime,
            final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion,
            final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis,
            final boolean consistentReads,
            final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts,
            final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds,
            final int cacheMissWarningModulus,
            final long initialLeaseTableReadCapacity,
            final long initialLeaseTableWriteCapacity,
            final HierarchicalShardSyncer hierarchicalShardSyncer,
            final TableCreatorCallback tableCreatorCallback,
            Duration dynamoDbRequestTimeout,
            BillingMode billingMode,
            Collection<Tag> tags) {

        this(
                kinesisClient,
                dynamoDBClient,
                tableName,
                workerIdentifier,
                executorService,
                failoverTimeMillis,
                LeaseManagementConfig.DEFAULT_ENABLE_PRIORITY_LEASE_ASSIGNMENT,
                epsilonMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime,
                maxLeaseRenewalThreads,
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardSyncIntervalMillis,
                consistentReads,
                listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts,
                maxCacheMissesBeforeReload,
                listShardsCacheAllowedAgeInSeconds,
                cacheMissWarningModulus,
                initialLeaseTableReadCapacity,
                initialLeaseTableWriteCapacity,
                hierarchicalShardSyncer,
                tableCreatorCallback,
                dynamoDbRequestTimeout,
                billingMode,
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
                tags,
                new DynamoDBLeaseSerializer(),
                null,
                false,
                LeaseManagementConfig.DEFAULT_LEASE_CLEANUP_CONFIG,
                new LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig(),
                LeaseManagementConfig.GracefulLeaseHandoffConfig.builder().build());
        this.streamConfig =
                new StreamConfig(StreamIdentifier.singleStreamInstance(streamName), initialPositionInStream);
    }

    /**
     * Constructor.
     * @param kinesisClient
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param failoverTimeMillis
     * @param enablePriorityLeaseAssignment
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
     * @param deprecatedHierarchicalShardSyncer
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     * @param billingMode
     * @param leaseTableDeletionProtectionEnabled
     * @param leaseSerializer
     * @param customShardDetectorProvider
     * @param isMultiStreamMode
     * @param leaseCleanupConfig
     */
    @Deprecated
    public DynamoDBLeaseManagementFactory(
            final KinesisAsyncClient kinesisClient,
            final DynamoDbAsyncClient dynamoDBClient,
            final String tableName,
            final String workerIdentifier,
            final ExecutorService executorService,
            final long failoverTimeMillis,
            final boolean enablePriorityLeaseAssignment,
            final long epsilonMillis,
            final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime,
            final int maxLeaseRenewalThreads,
            final boolean cleanupLeasesUponShardCompletion,
            final boolean ignoreUnexpectedChildShards,
            final long shardSyncIntervalMillis,
            final boolean consistentReads,
            final long listShardsBackoffTimeMillis,
            final int maxListShardsRetryAttempts,
            final int maxCacheMissesBeforeReload,
            final long listShardsCacheAllowedAgeInSeconds,
            final int cacheMissWarningModulus,
            final long initialLeaseTableReadCapacity,
            final long initialLeaseTableWriteCapacity,
            final HierarchicalShardSyncer deprecatedHierarchicalShardSyncer,
            final TableCreatorCallback tableCreatorCallback,
            Duration dynamoDbRequestTimeout,
            BillingMode billingMode,
            final boolean leaseTableDeletionProtectionEnabled,
            final boolean leaseTablePitrEnabled,
            Collection<Tag> tags,
            LeaseSerializer leaseSerializer,
            Function<StreamConfig, ShardDetector> customShardDetectorProvider,
            boolean isMultiStreamMode,
            LeaseCleanupConfig leaseCleanupConfig,
            final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig,
            final LeaseManagementConfig.GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig) {
        this.kinesisClient = kinesisClient;
        this.dynamoDBClient = dynamoDBClient;
        this.tableName = tableName;
        this.workerIdentifier = workerIdentifier;
        this.executorService = executorService;
        this.failoverTimeMillis = failoverTimeMillis;
        this.enablePriorityLeaseAssignment = enablePriorityLeaseAssignment;
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
        this.deprecatedHierarchicalShardSyncer = deprecatedHierarchicalShardSyncer;
        this.tableCreatorCallback = tableCreatorCallback;
        this.dynamoDbRequestTimeout = dynamoDbRequestTimeout;
        this.billingMode = billingMode;
        this.leaseTableDeletionProtectionEnabled = leaseTableDeletionProtectionEnabled;
        this.leaseTablePitrEnabled = leaseTablePitrEnabled;
        this.leaseSerializer = leaseSerializer;
        this.customShardDetectorProvider = customShardDetectorProvider;
        this.isMultiStreamMode = isMultiStreamMode;
        this.leaseCleanupConfig = leaseCleanupConfig;
        this.tags = tags;
        this.workerUtilizationAwareAssignmentConfig = workerUtilizationAwareAssignmentConfig;
        this.gracefulLeaseHandoffConfig = gracefulLeaseHandoffConfig;
    }

    @Override
    public LeaseCoordinator createLeaseCoordinator(@NonNull final MetricsFactory metricsFactory) {
        return createLeaseCoordinator(metricsFactory, new ConcurrentHashMap<>());
    }

    @Override
    public LeaseCoordinator createLeaseCoordinator(
            @NonNull final MetricsFactory metricsFactory,
            @NonNull final ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap) {
        return new DynamoDBLeaseCoordinator(
                this.createLeaseRefresher(),
                workerIdentifier,
                failoverTimeMillis,
                enablePriorityLeaseAssignment,
                epsilonMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime,
                maxLeaseRenewalThreads,
                initialLeaseTableReadCapacity,
                initialLeaseTableWriteCapacity,
                metricsFactory,
                workerUtilizationAwareAssignmentConfig,
                gracefulLeaseHandoffConfig,
                shardInfoShardConsumerMap);
    }

    /**
     * Even though this is deprecated, this is a method part of the public interface in LeaseManagementFactory
     */
    @Override
    @Deprecated
    public ShardSyncTaskManager createShardSyncTaskManager(@NonNull final MetricsFactory metricsFactory) {
        return new ShardSyncTaskManager(
                this.createShardDetector(),
                this.createLeaseRefresher(),
                streamConfig.initialPositionInStreamExtended(),
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardSyncIntervalMillis,
                executorService,
                deprecatedHierarchicalShardSyncer,
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
        return createShardSyncTaskManager(metricsFactory, streamConfig, null);
    }

    /**
     * Create ShardSyncTaskManager from the streamConfig passed
     *
     * @param metricsFactory - factory to get metrics object
     * @param streamConfig - streamConfig for which ShardSyncTaskManager needs to be created
     * @param deletedStreamListProvider - store for capturing the streams which are deleted in kinesis
     * @return ShardSyncTaskManager
     */
    @Override
    public ShardSyncTaskManager createShardSyncTaskManager(
            MetricsFactory metricsFactory,
            StreamConfig streamConfig,
            DeletedStreamListProvider deletedStreamListProvider) {
        return new ShardSyncTaskManager(
                this.createShardDetector(streamConfig),
                this.createLeaseRefresher(),
                streamConfig.initialPositionInStreamExtended(),
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardSyncIntervalMillis,
                executorService,
                new HierarchicalShardSyncer(
                        isMultiStreamMode, streamConfig.streamIdentifier().toString(), deletedStreamListProvider),
                metricsFactory);
    }

    @Override
    public DynamoDBLeaseRefresher createLeaseRefresher() {
        final DdbTableConfig ddbTableConfig = new DdbTableConfig();
        ddbTableConfig.billingMode(billingMode);
        ddbTableConfig.readCapacity(initialLeaseTableReadCapacity);
        ddbTableConfig.writeCapacity(initialLeaseTableWriteCapacity);
        return new DynamoDBLeaseRefresher(
                tableName,
                dynamoDBClient,
                leaseSerializer,
                consistentReads,
                tableCreatorCallback,
                dynamoDbRequestTimeout,
                ddbTableConfig,
                leaseTableDeletionProtectionEnabled,
                leaseTablePitrEnabled,
                tags);
    }

    /**
     *  Even though this is deprecated, this is a method part of the public interface in LeaseManagementFactory
     */
    @Override
    @Deprecated
    public ShardDetector createShardDetector() {
        return new KinesisShardDetector(
                kinesisClient,
                streamConfig.streamIdentifier(),
                listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts,
                listShardsCacheAllowedAgeInSeconds,
                maxCacheMissesBeforeReload,
                cacheMissWarningModulus,
                dynamoDbRequestTimeout);
    }

    /**
     * KinesisShardDetector supports reading from service only using streamName. Support for accountId and
     * stream creation epoch is yet to be provided.
     * @param streamConfig
     * @return ShardDetector
     */
    @Override
    public ShardDetector createShardDetector(StreamConfig streamConfig) {
        return customShardDetectorProvider != null
                ? customShardDetectorProvider.apply(streamConfig)
                : new KinesisShardDetector(
                        kinesisClient,
                        streamConfig.streamIdentifier(),
                        listShardsBackoffTimeMillis,
                        maxListShardsRetryAttempts,
                        listShardsCacheAllowedAgeInSeconds,
                        maxCacheMissesBeforeReload,
                        cacheMissWarningModulus,
                        dynamoDbRequestTimeout);
    }

    /**
     * LeaseCleanupManager cleans up leases in the lease table for shards which have either expired past the
     * stream's retention period or have been completely processed.
     * @param metricsFactory
     * @return LeaseCleanupManager
     */
    @Override
    public LeaseCleanupManager createLeaseCleanupManager(MetricsFactory metricsFactory) {
        return new LeaseCleanupManager(
                createLeaseCoordinator(metricsFactory),
                metricsFactory,
                Executors.newSingleThreadScheduledExecutor(),
                cleanupLeasesUponShardCompletion,
                leaseCleanupConfig.leaseCleanupIntervalMillis(),
                leaseCleanupConfig.completedLeaseCleanupIntervalMillis(),
                leaseCleanupConfig.garbageLeaseCleanupIntervalMillis());
    }
}
