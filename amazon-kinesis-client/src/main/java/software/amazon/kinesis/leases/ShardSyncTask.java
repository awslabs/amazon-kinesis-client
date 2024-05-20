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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.TaskType;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
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
    private static final String SHARD_SYNC_TASK_OPERATION = "ShardSyncTask";

    @NonNull
    private final ShardDetector shardDetector;

    @NonNull
    private final LeaseRefresher leaseRefresher;

    @NonNull
    private final InitialPositionInStreamExtended initialPosition;

    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean garbageCollectLeases;
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
        boolean shardSyncSuccess = true;

        try {
            boolean didPerformShardSync = hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    leaseRefresher,
                    initialPosition,
                    scope,
                    ignoreUnexpectedChildShards,
                    leaseRefresher.isLeaseTableEmpty());

            if (didPerformShardSync && shardSyncTaskIdleTimeMillis > 0) {
                Thread.sleep(shardSyncTaskIdleTimeMillis);
            }
        } catch (Exception e) {
            log.error("Caught exception while sync'ing Kinesis shards and leases", e);
            exception = e;
            shardSyncSuccess = false;
        } finally {
            // NOTE: This metric is reflecting if a shard sync task succeeds. Customer can use this metric to monitor if
            // their application encounter any shard sync failures. This metric can help to detect potential shard stuck
            // issues
            // that are due to shard sync failures.
            MetricsUtil.addSuccess(scope, "SyncShards", shardSyncSuccess, MetricsLevel.DETAILED);
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
