/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.kinesis.leases.dynamodb;

import java.util.concurrent.ExecutorService;

import lombok.Data;
import lombok.NonNull;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.KinesisShardDetector;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementFactory;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.metrics.MetricsFactory;

/**
 *
 */
@Data
public class DynamoDBLeaseManagementFactory implements LeaseManagementFactory {
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final String streamName;
    @NonNull
    private final DynamoDbAsyncClient dynamoDBClient;
    @NonNull
    private final String tableName;
    @NonNull
    private final String workerIdentifier;
    @NonNull
    private final ExecutorService executorService;
    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;
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

    @Override
    public LeaseCoordinator createLeaseCoordinator(@NonNull final MetricsFactory metricsFactory) {
        return new DynamoDBLeaseCoordinator(this.createLeaseRefresher(),
                workerIdentifier,
                failoverTimeMillis,
                epsilonMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime,
                maxLeaseRenewalThreads,
                metricsFactory);
    }

    @Override
    public ShardSyncTaskManager createShardSyncTaskManager(@NonNull final MetricsFactory metricsFactory) {
        return new ShardSyncTaskManager(this.createShardDetector(),
                this.createLeaseRefresher(),
                initialPositionInStream,
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardSyncIntervalMillis,
                executorService,
                metricsFactory);
    }

    @Override
    public DynamoDBLeaseRefresher createLeaseRefresher() {
        return new DynamoDBLeaseRefresher(tableName, dynamoDBClient, new DynamoDBLeaseSerializer(), consistentReads);
    }

    @Override
    public ShardDetector createShardDetector() {
        return new KinesisShardDetector(kinesisClient, streamName, listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts, listShardsCacheAllowedAgeInSeconds, maxCacheMissesBeforeReload,
                cacheMissWarningModulus);
    }
}
