/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import software.amazon.kinesis.lifecycle.ITask;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.retrieval.IKinesisProxy;
import software.amazon.kinesis.leases.KinesisClientLease;
import software.amazon.kinesis.leases.ILeaseManager;
import software.amazon.kinesis.metrics.IMetricsFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * The ShardSyncTaskManager is used to track the task to sync shards with leases (create leases for new
 * Kinesis shards, remove obsolete leases). We'll have at most one outstanding sync task at any time.
 * Worker will use this class to kick off a sync task when it finds shards which have been completely processed.
 */
@Slf4j
class ShardSyncTaskManager {
    private ITask currentTask;
    private Future<TaskResult> future;
    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final IMetricsFactory metricsFactory;
    private final ExecutorService executorService;
    private final InitialPositionInStreamExtended initialPositionInStream;
    private boolean cleanupLeasesUponShardCompletion;
    private boolean ignoreUnexpectedChildShards;
    private final long shardSyncIdleTimeMillis;


    /**
     * Constructor.
     * 
     * @param kinesisProxy Proxy used to fetch streamInfo (shards)
     * @param leaseManager Lease manager (used to list and create leases for shards)
     * @param initialPositionInStream Initial position in stream
     * @param cleanupLeasesUponShardCompletion Clean up leases for shards that we've finished processing (don't wait
     *        until they expire)
     * @param ignoreUnexpectedChildShards Ignore child shards with open parents
     * @param shardSyncIdleTimeMillis Time between tasks to sync leases and Kinesis shards
     * @param metricsFactory Metrics factory
     * @param executorService ExecutorService to execute the shard sync tasks
     */
    ShardSyncTaskManager(final IKinesisProxy kinesisProxy,
            final ILeaseManager<KinesisClientLease> leaseManager,
            final InitialPositionInStreamExtended initialPositionInStream,
            final boolean cleanupLeasesUponShardCompletion,
            final boolean ignoreUnexpectedChildShards,
            final long shardSyncIdleTimeMillis,
            final IMetricsFactory metricsFactory,
            ExecutorService executorService) {
        this.kinesisProxy = kinesisProxy;
        this.leaseManager = leaseManager;
        this.metricsFactory = metricsFactory;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncIdleTimeMillis = shardSyncIdleTimeMillis;
        this.executorService = executorService;
        this.initialPositionInStream = initialPositionInStream;
    }

    synchronized boolean syncShardAndLeaseInfo(Set<String> closedShardIds) {
        return checkAndSubmitNextTask(closedShardIds);
    }

    private synchronized boolean checkAndSubmitNextTask(Set<String> closedShardIds) {
        boolean submittedNewTask = false;
        if ((future == null) || future.isCancelled() || future.isDone()) {
            if ((future != null) && future.isDone()) {
                try {
                    TaskResult result = future.get();
                    if (result.getException() != null) {
                        log.error("Caught exception running {} task: ", currentTask.getTaskType(),
                                result.getException());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.warn("{} task encountered exception.", currentTask.getTaskType(), e);
                }
            }

            currentTask =
                    new MetricsCollectingTaskDecorator(new ShardSyncTask(kinesisProxy,
                            leaseManager,
                            initialPositionInStream,
                            cleanupLeasesUponShardCompletion,
                            ignoreUnexpectedChildShards,
                            shardSyncIdleTimeMillis), metricsFactory);
            future = executorService.submit(currentTask);
            submittedNewTask = true;
            if (log.isDebugEnabled()) {
                log.debug("Submitted new {} task.", currentTask.getTaskType());
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Previous {} task still pending.  Not submitting new task.", currentTask.getTaskType());
            }
        }

        return submittedNewTask;
    }

}
