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

package software.amazon.kinesis.leases;

import java.util.concurrent.ExecutorService;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;

import lombok.Data;
import lombok.NonNull;
import software.amazon.kinesis.metrics.IMetricsFactory;

/**
 *
 */
@Data
public class DynamoDBLeaseManagementFactory implements LeaseManagementFactory {
    @NonNull
    private final AmazonKinesis amazonKinesis;
    @NonNull
    private final String streamName;
    @NonNull
    private final AmazonDynamoDB amazonDynamoDB;
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
    @NonNull
    private final IMetricsFactory metricsFactory;

    @Override
    public LeaseCoordinator createLeaseCoordinator() {
        return createKinesisClientLibLeaseCoordinator();
    }

    @Override
    public ShardSyncTaskManager createShardSyncTaskManager() {
        return new ShardSyncTaskManager(this.createLeaseManagerProxy(),
                this.createLeaseManager(),
                initialPositionInStream,
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardSyncIntervalMillis,
                executorService,
                metricsFactory);
    }

    @Override
    public DynamoDBLeaseManager<KinesisClientLease> createLeaseManager() {
        return new KinesisClientDynamoDBLeaseManager(tableName, amazonDynamoDB, consistentReads);
    }

    @Override
    public KinesisClientLibLeaseCoordinator createKinesisClientLibLeaseCoordinator() {
        return new KinesisClientLibLeaseCoordinator(this.createLeaseManager(),
                workerIdentifier,
                failoverTimeMillis,
                epsilonMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime,
                maxLeaseRenewalThreads,
                metricsFactory);
    }

    @Override
    public LeaseManagerProxy createLeaseManagerProxy() {
        return new KinesisLeaseManagerProxy(amazonKinesis, streamName, listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts);
    }
}
