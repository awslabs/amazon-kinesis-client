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

package software.amazon.kinesis.lifecycle;

import java.util.concurrent.ExecutorService;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.HashKeyRangeForLease;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.schemaregistry.SchemaRegistryDecoder;

@Data
@Slf4j
@Accessors(fluent = true)
@KinesisClientInternalApi
public class ShardConsumerArgument {
    @NonNull
    private final ShardInfo shardInfo;

    @NonNull
    private final StreamIdentifier streamIdentifier;

    @NonNull
    private final LeaseCoordinator leaseCoordinator;

    @NonNull
    private final ExecutorService executorService;

    @NonNull
    private final RecordsPublisher recordsPublisher;

    @NonNull
    private final ShardRecordProcessor shardRecordProcessor;

    @NonNull
    private final Checkpointer checkpoint;

    @NonNull
    private final ShardRecordProcessorCheckpointer recordProcessorCheckpointer;

    private final long parentShardPollIntervalMillis;
    private final long taskBackoffTimeMillis;
    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist;
    private final long listShardsBackoffTimeInMillis;
    private final int maxListShardsRetryAttempts;
    private final boolean shouldCallProcessRecordsEvenForEmptyRecordList;
    private final long idleTimeInMilliseconds;

    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;

    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;

    @NonNull
    private final ShardDetector shardDetector;

    private final AggregatorUtil aggregatorUtil;
    private final HierarchicalShardSyncer hierarchicalShardSyncer;

    @NonNull
    private final MetricsFactory metricsFactory;

    private final LeaseCleanupManager leaseCleanupManager;
    private final SchemaRegistryDecoder schemaRegistryDecoder;

    /**
     * Consumer ID generated from lease table ARN to uniquely identify this KCL application
     */
    private String consumerId;

    /** Hash key range cached by {@link #hashKeyRange()} once resolved; null until then. */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private volatile HashKeyRangeForLease cachedHashKeyRange;

    public ShardConsumerArgument(
            @NonNull ShardInfo shardInfo,
            @NonNull StreamIdentifier streamIdentifier,
            LeaseCoordinator leaseCoordinator,
            ExecutorService executorService,
            RecordsPublisher recordsPublisher,
            ShardRecordProcessor shardRecordProcessor,
            Checkpointer checkpoint,
            ShardRecordProcessorCheckpointer recordProcessorCheckpointer,
            long parentShardPollIntervalMillis,
            long taskBackoffTimeMillis,
            boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
            long listShardsBackoffTimeMillis,
            int maxListShardsRetryAttempts,
            boolean shouldCallProcessRecordsEvenForEmptyRecordList,
            long idleTimeInMilliseconds,
            InitialPositionInStreamExtended initialPositionInStream,
            boolean cleanupLeasesUponShardCompletion,
            boolean ignoreUnexpectedChildShards,
            ShardDetector shardDetector,
            AggregatorUtil aggregatorUtil,
            HierarchicalShardSyncer hierarchicalShardSyncer,
            MetricsFactory metricsFactory,
            @NonNull LeaseCleanupManager leaseCleanupManager,
            SchemaRegistryDecoder schemaRegistryDecoder) {
        this(
                shardInfo,
                streamIdentifier,
                leaseCoordinator,
                executorService,
                recordsPublisher,
                shardRecordProcessor,
                checkpoint,
                recordProcessorCheckpointer,
                parentShardPollIntervalMillis,
                taskBackoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts,
                shouldCallProcessRecordsEvenForEmptyRecordList,
                idleTimeInMilliseconds,
                initialPositionInStream,
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardDetector,
                aggregatorUtil,
                hierarchicalShardSyncer,
                metricsFactory,
                leaseCleanupManager,
                schemaRegistryDecoder,
                null);
    }

    public ShardConsumerArgument(
            @NonNull ShardInfo shardInfo,
            @NonNull StreamIdentifier streamIdentifier,
            LeaseCoordinator leaseCoordinator,
            ExecutorService executorService,
            RecordsPublisher recordsPublisher,
            ShardRecordProcessor shardRecordProcessor,
            Checkpointer checkpoint,
            ShardRecordProcessorCheckpointer recordProcessorCheckpointer,
            long parentShardPollIntervalMillis,
            long taskBackoffTimeMillis,
            boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
            long listShardsBackoffTimeMillis,
            int maxListShardsRetryAttempts,
            boolean shouldCallProcessRecordsEvenForEmptyRecordList,
            long idleTimeInMilliseconds,
            InitialPositionInStreamExtended initialPositionInStream,
            boolean cleanupLeasesUponShardCompletion,
            boolean ignoreUnexpectedChildShards,
            ShardDetector shardDetector,
            AggregatorUtil aggregatorUtil,
            HierarchicalShardSyncer hierarchicalShardSyncer,
            MetricsFactory metricsFactory,
            @NonNull LeaseCleanupManager leaseCleanupManager,
            SchemaRegistryDecoder schemaRegistryDecoder,
            String consumerId) {
        this.shardInfo = shardInfo;
        this.streamIdentifier = streamIdentifier;
        this.leaseCoordinator = leaseCoordinator;
        this.executorService = executorService;
        this.recordsPublisher = recordsPublisher;
        this.shardRecordProcessor = shardRecordProcessor;
        this.checkpoint = checkpoint;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
        this.skipShardSyncAtWorkerInitializationIfLeasesExist = skipShardSyncAtWorkerInitializationIfLeasesExist;
        this.listShardsBackoffTimeInMillis = listShardsBackoffTimeMillis;
        this.maxListShardsRetryAttempts = maxListShardsRetryAttempts;
        this.shouldCallProcessRecordsEvenForEmptyRecordList = shouldCallProcessRecordsEvenForEmptyRecordList;
        this.idleTimeInMilliseconds = idleTimeInMilliseconds;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesUponShardCompletion;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardDetector = shardDetector;
        this.aggregatorUtil = aggregatorUtil;
        this.hierarchicalShardSyncer = hierarchicalShardSyncer;
        this.metricsFactory = metricsFactory;
        this.leaseCleanupManager = leaseCleanupManager;
        this.schemaRegistryDecoder = schemaRegistryDecoder;
        this.consumerId = consumerId;
    }

    /**
     * Returns the shard's hash key range, used to drop KPL user records outside the shard during deaggregation, or
     * null to deaggregate without range filtering.
     * <p>
     * As before, no range filtering is done when {@code skipShardSyncAtWorkerInitializationIfLeasesExist} is set.
     * Otherwise the range comes from the {@link ShardInfo}, which copies it from the lease when the assignment is
     * created, falling back to the {@link ShardDetector} only if it is not set there. Cached once resolved; returns
     * null (not cached) if it cannot be resolved yet.
     */
    public HashKeyRangeForLease hashKeyRange() {
        if (skipShardSyncAtWorkerInitializationIfLeasesExist) {
            return null;
        }
        final HashKeyRangeForLease cached = cachedHashKeyRange;
        if (cached != null) {
            return cached;
        }

        HashKeyRangeForLease range = shardInfo.hashKeyRange();
        if (range == null) {
            // Rarely reached: KCL has stored the hash key range on every lease it creates since 2.3, and the lease
            // coordinator copies it into the ShardInfo. Fall back to the shard detector, which may call ListShards,
            // only for a lease created by an older KCL (1.x or 2.x before 2.3) that the leader's
            // PeriodicShardSyncManager has not backfilled yet, or a custom lease coordinator that does not set the
            // range. Failures are caught and retried on the next batch.
            range = hashKeyRangeFromShardDetector();
            if (range == null) {
                return null;
            }
        }

        cachedHashKeyRange = range;
        return range;
    }

    private HashKeyRangeForLease hashKeyRangeFromShardDetector() {
        final String leaseKey = ShardInfo.getLeaseKey(shardInfo);
        try {
            final Shard shard = shardDetector.shard(shardInfo.shardId());
            if (shard != null && shard.hashKeyRange() != null) {
                return HashKeyRangeForLease.fromHashKeyRange(shard.hashKeyRange());
            }
            log.warn(
                    "{}: Cannot get the shard, so duplicate KPL user records in the event of resharding will not be"
                            + " dropped during deaggregation of Amazon Kinesis records.",
                    leaseKey);
        } catch (RuntimeException e) {
            log.warn(
                    "{}: Failed to get the shard hash key range, so duplicate KPL user records in the event of"
                            + " resharding will not be dropped during deaggregation of Amazon Kinesis records.",
                    leaseKey,
                    e);
        }
        return null;
    }
}
