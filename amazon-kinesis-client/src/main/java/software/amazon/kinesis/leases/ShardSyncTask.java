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
package software.amazon.kinesis.leases;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.TaskType;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

/**
 * This task syncs leases/activies with shards of the stream.
 * It will create new leases/activites when it discovers new shards (e.g. setup/resharding).
 * It will clean up leases/activities for shards that have been completely processed (if
 * cleanupLeasesUponShardCompletion is true).
 */
@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class ShardSyncTask implements ConsumerTask {
    private final String SHARD_SYNC_TASK_OPERATION = "ShardSyncTask";

    @NonNull
    private final ShardDetector shardDetector;
    @NonNull
    private final LeaseRefresher leaseRefresher;
    @NonNull
    private final InitialPositionInStreamExtended initialPosition;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean ignoreUnexpectedChildShards;
    private final long shardSyncTaskIdleTimeMillis;
    @NonNull
    private final HierarchicalShardSyncer hierarchicalShardSyncer;
    @NonNull
    private final MetricsFactory metricsFactory;

    private final TaskType taskType = TaskType.SHARDSYNC;

    /*
     * (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#call()
     */
    @Override
    public TaskResult call() {
        Exception exception = null;
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, SHARD_SYNC_TASK_OPERATION);

        try {
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(shardDetector, leaseRefresher, initialPosition,
                    cleanupLeasesUponShardCompletion, ignoreUnexpectedChildShards, scope);
            if (shardSyncTaskIdleTimeMillis > 0) {
                Thread.sleep(shardSyncTaskIdleTimeMillis);
            }
        } catch (Exception e) {
            log.error("Caught exception while sync'ing Kinesis shards and leases", e);
            exception = e;
        } finally {
            MetricsUtil.endScope(scope);
        }

        return new TaskResult(exception);
    }

    /*
     * (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#taskType()
     */
    @Override
    public TaskType taskType() {
        return taskType;
    }

}
