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

    /** Hash key range cached by {@link #hashKeyRange()} once resolved; null until then. */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private HashKeyRangeForLease cachedHashKeyRange;

    /**
     * Whether to drop KPL user records outside the shard's hash key range (duplicates after a reshard). Previously
     * ProcessTask only looked up the range, via ListShards, when this flag was not set, so trimming stays tied to it.
     */
    public boolean shouldTrimKplRecordsOutsideHashKeyRange() {
        return !skipShardSyncAtWorkerInitializationIfLeasesExist;
    }

    /**
     * Returns the shard's hash key range from the {@link ShardInfo} (copied from the lease), falling back to the
     * {@link ShardDetector} only when trimming is enabled. Cached once resolved; null if not known.
     */
    public HashKeyRangeForLease hashKeyRange() {
        final HashKeyRangeForLease cached = cachedHashKeyRange;
        if (cached != null) {
            return cached;
        }

        HashKeyRangeForLease range = shardInfo.hashKeyRange();
        if (range == null) {
            if (!shouldTrimKplRecordsOutsideHashKeyRange()) {
                // The fallback may call ListShards; only worth it when the range is used for trimming.
                return null;
            }
            // Rare: legacy lease (pre-2.3, not yet backfilled) or a coordinator that doesn't set the range. A failure
            // propagates so the batch is retried rather than delivered unfiltered.
            range = hashKeyRangeFromShardDetector();
            if (range == null) {
                return null;
            }
        }

        cachedHashKeyRange = range;
        return range;
    }

    private HashKeyRangeForLease hashKeyRangeFromShardDetector() {
        final Shard shard = shardDetector.shard(shardInfo.shardId());
        if (shard != null && shard.hashKeyRange() != null) {
            return HashKeyRangeForLease.fromHashKeyRange(shard.hashKeyRange());
        }
        log.warn(
                "{}: Cannot get the shard, so duplicate KPL user records in the event of resharding will not be"
                        + " dropped during deaggregation of Amazon Kinesis records.",
                ShardInfo.getLeaseKey(shardInfo));
        return null;
    }
}
