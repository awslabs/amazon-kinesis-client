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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import software.amazon.kinesis.common.InitialPositionInStreamExtended;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsCollectingTaskDecorator;

/**
 * The ShardSyncTaskManager is used to track the task to sync shards with leases (create leases for new
 * Kinesis shards, remove obsolete leases). We'll have at most one outstanding sync task at any time.
 * Worker will use this class to kick off a sync task when it finds shards which have been completely processed.
 */
@Data
@Accessors(fluent = true)
@Slf4j
public class ShardSyncTaskManager {
    @NonNull
    private final ShardDetector shardDetector;
    @NonNull
    private final LeaseRefresher leaseRefresher;
    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean ignoreUnexpectedChildShards;
    private final long shardSyncIdleTimeMillis;
    @NonNull
    private final ExecutorService executorService;
    @NonNull
    private final ShardSyncer shardSyncer;
    @NonNull
    private final MetricsFactory metricsFactory;

    public ShardSyncTaskManager(ShardDetector shardDetector, LeaseRefresher leaseRefresher, InitialPositionInStreamExtended initialPositionInStream,
                                boolean cleanupLeasesUponShardCompletion, boolean ignoreUnexpectedChildShards, long shardSyncIdleTimeMillis,
                                ExecutorService executorService, ShardSyncer shardSyncer, MetricsFactory metricsFactory) {
        this.shardDetector = shardDetector;
        this.leaseRefresher = leaseRefresher;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncIdleTimeMillis = shardSyncIdleTimeMillis;
        this.executorService = executorService;
        this.shardSyncer = shardSyncer;
        this.metricsFactory = metricsFactory;
    }

    private ConsumerTask currentTask;
    private Future<TaskResult> future;

    public synchronized boolean syncShardAndLeaseInfo() {
        return checkAndSubmitNextTask();
    }

    private synchronized boolean checkAndSubmitNextTask() {
        boolean submittedNewTask = false;
        if ((future == null) || future.isCancelled() || future.isDone()) {
            if ((future != null) && future.isDone()) {
                try {
                    TaskResult result = future.get();
                    if (result.getException() != null) {
                        log.error("Caught exception running {} task: ", currentTask.taskType(),
                                result.getException());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.warn("{} task encountered exception.", currentTask.taskType(), e);
                }
            }

            currentTask =
                    new MetricsCollectingTaskDecorator(
                            new ShardSyncTask(shardDetector,
                                    leaseRefresher,
                                    initialPositionInStream,
                                    cleanupLeasesUponShardCompletion,
                                    ignoreUnexpectedChildShards,
                                    shardSyncIdleTimeMillis,
                                    shardSyncer,
                                    metricsFactory),
                            metricsFactory);
            future = executorService.submit(currentTask);
            submittedNewTask = true;
            if (log.isDebugEnabled()) {
                log.debug("Submitted new {} task.", currentTask.taskType());
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Previous {} task still pending.  Not submitting new task.", currentTask.taskType());
            }
        }

        return submittedNewTask;
    }

}
