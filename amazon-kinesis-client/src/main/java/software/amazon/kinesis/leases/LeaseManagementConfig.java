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
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;

/**
 * Used by the KCL to configure lease management.
 */
@Data
@Accessors(fluent = true)
public class LeaseManagementConfig {
    public static final int DEFAULT_MAX_LEASE_RENEWAL_THREADS = 20;

    /**
     * Name of the table to use in DynamoDB
     *
     * @return String
     */
    @NonNull
    private final String tableName;
    /**
     * Client to be used to access DynamoDB service.
     *
     * @return {@link AmazonDynamoDB}
     */
    @NonNull
    private final AmazonDynamoDB amazonDynamoDB;
    /**
     * Client to be used to access Kinesis Data Streams service.
     *
     * @return {@link AmazonKinesis}
     */
    @NonNull
    private final AmazonKinesis amazonKinesis;
    /**
     * Name of the Kinesis Data Stream to read records from.
     */
    @NonNull
    private final String streamName;
    /**
     * Used to distinguish different workers/processes of a KCL application.
     *
     * @return String
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
     * The max number of leases (shards) this worker should process.
     * This can be useful to avoid overloading (and thrashing) a worker when a host has resource constraints
     * or during deployment.
     *
     * <p>NOTE: Setting this to a low value can cause data loss if workers are not able to pick up all shards in the
     * stream due to the max limit.</p>
     *
     * <p>Default value: {@link Integer#MAX_VALUE}</p>
     */
    private int maxLeasesForWorker = Integer.MAX_VALUE;;

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

    /**
     * The initial position for getting records from Kinesis streams.
     *
     * <p>Default value: {@link InitialPositionInStream#TRIM_HORIZON}</p>
     */
    private InitialPositionInStreamExtended initialPositionInStream =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    /**
     *
     */
    private IMetricsFactory metricsFactory = new NullMetricsFactory();

    /**
     * The {@link ExecutorService} to be used by {@link ShardSyncTaskManager}.
     *
     * <p>Default value: {@link LeaseManagementThreadPool}</p>
     */
    private ExecutorService executorService = new LeaseManagementThreadPool(
            new ThreadFactoryBuilder().setNameFormat("ShardSyncTaskManager-%04d").build());

    static class LeaseManagementThreadPool extends ThreadPoolExecutor {
        private static final long DEFAULT_KEEP_ALIVE_TIME = 60L;

        LeaseManagementThreadPool(ThreadFactory threadFactory) {
            super(0, Integer.MAX_VALUE, DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    threadFactory);
        }
    };

    private LeaseManagementFactory leaseManagementFactory;

    public LeaseManagementFactory leaseManagementFactory() {
        if (leaseManagementFactory == null) {
            leaseManagementFactory = new DynamoDBLeaseManagementFactory(amazonKinesis(),
                    streamName(),
                    amazonDynamoDB(),
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
                    metricsFactory());
        }
        return leaseManagementFactory;
    }

}
