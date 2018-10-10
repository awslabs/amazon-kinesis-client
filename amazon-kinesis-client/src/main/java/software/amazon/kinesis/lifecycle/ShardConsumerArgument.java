/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.kinesis.lifecycle;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.RecordsPublisher;

import java.util.concurrent.ExecutorService;

@Data
@Accessors(fluent = true)
@KinesisClientInternalApi
public class ShardConsumerArgument {
    @NonNull
    private final ShardInfo shardInfo;
    @NonNull
    private final String streamName;
    @NonNull
    private final LeaseRefresher leaseRefresher;
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
}
