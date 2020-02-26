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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.amazonaws.services.kinesis.model.Shard;
import lombok.Getter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

/**
 * The ShardSyncTaskManager is used to track the task to sync shards with leases (create leases for new
 * Kinesis shards, remove obsolete leases). We'll have at most one outstanding sync task at any time.
 * Worker will use this class to kick off a sync task when it finds shards which have been completely processed.
 */
@Getter class ShardSyncTaskManager {

    private static final Log LOG = LogFactory.getLog(ShardSyncTaskManager.class);

    private ITask currentTask;
    private CompletableFuture<TaskResult> future;
    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final IMetricsFactory metricsFactory;
    private final ExecutorService executorService;
    private final InitialPositionInStreamExtended initialPositionInStream;
    private boolean cleanupLeasesUponShardCompletion;
    private boolean ignoreUnexpectedChildShards;
    private final long shardSyncIdleTimeMillis;
    private final ShardSyncer shardSyncer;
    private final ReentrantLock lock;

    private AtomicBoolean shardSyncRequestPending;

    /**
     * Constructor.
     *
     * @param kinesisProxy                     Proxy used to fetch streamInfo (shards)
     * @param leaseManager                     Lease manager (used to list and create leases for shards)
     * @param initialPositionInStream          Initial position in stream
     * @param cleanupLeasesUponShardCompletion Clean up leases for shards that we've finished processing (don't wait
     *                                         until they expire)
     * @param ignoreUnexpectedChildShards      Ignore child shards with open parents
     * @param shardSyncIdleTimeMillis          Time between tasks to sync leases and Kinesis shards
     * @param metricsFactory                   Metrics factory
     * @param executorService                  ExecutorService to execute the shard sync tasks
     * @param shardSyncer                      shardSyncer instance used to check and create new leases
     */
    ShardSyncTaskManager(final IKinesisProxy kinesisProxy, final ILeaseManager<KinesisClientLease> leaseManager,
            final InitialPositionInStreamExtended initialPositionInStream,
            final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
            final long shardSyncIdleTimeMillis, final IMetricsFactory metricsFactory, ExecutorService executorService,
            ShardSyncer shardSyncer) {
        this.kinesisProxy = kinesisProxy;
        this.leaseManager = leaseManager;
        this.metricsFactory = metricsFactory;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncIdleTimeMillis = shardSyncIdleTimeMillis;
        this.executorService = executorService;
        this.initialPositionInStream = initialPositionInStream;
        this.shardSyncer = shardSyncer;
        this.shardSyncRequestPending = new AtomicBoolean(false);
        this.lock = new ReentrantLock();
    }

    Future<TaskResult> syncShardAndLeaseInfo(List<Shard> latestShards) {
        try {
            lock.lock();
            return checkAndSubmitNextTask(latestShards);
        } finally {
            lock.unlock();
        }
    }

    private Future<TaskResult> checkAndSubmitNextTask(List<Shard> latestShards) {
        Future<TaskResult> submittedTaskFuture = null;
        if ((future == null) || future.isCancelled() || future.isDone()) {
            if ((future != null) && future.isDone()) {
                try {
                    TaskResult result = future.get();
                    if (result.getException() != null) {
                        LOG.error("Caught exception running " + currentTask.getTaskType() + " task: ",
                                result.getException());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOG.warn(currentTask.getTaskType() + " task encountered exception.", e);
                }
            }

            currentTask = new MetricsCollectingTaskDecorator(
                    new ShardSyncTask(kinesisProxy, leaseManager, initialPositionInStream,
                            cleanupLeasesUponShardCompletion, ignoreUnexpectedChildShards, shardSyncIdleTimeMillis,
                            shardSyncer, latestShards), metricsFactory);
            future = CompletableFuture.supplyAsync(() -> currentTask.call(), executorService)
                    .whenComplete((taskResult, exception) -> handlePendingShardSyncs(exception, taskResult));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Submitted new " + currentTask.getTaskType() + " task.");
            }
            submittedTaskFuture = future;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Previous " + currentTask.getTaskType() + " task still pending.  Not submitting new task. "
                        + "Enqueued a request that will be executed when the current request completes.");
            }
            shardSyncRequestPending.compareAndSet(false /*expected*/, true /*update*/);
        }
        return submittedTaskFuture;
    }

    private void handlePendingShardSyncs(Throwable exception, TaskResult taskResult) {
        if (exception != null || taskResult.getException() != null) {
            LOG.error("Caught exception running " + currentTask.getTaskType() + " task: ", exception != null ? exception : taskResult.getException());
        }
        // Acquire lock here. If shardSyncRequestPending is false in this completionStage and
        // syncShardAndLeaseInfo is invoked, before completion stage exits (future completes)
        // but right after the value of shardSyncRequestPending is checked, it will result in
        // shardSyncRequestPending being set to true, but no pending futures to trigger the next
        // ShardSyncTask. By executing this stage in a Reentrant lock, we ensure that if the
        // previous task is in this completion stage, checkAndSubmitNextTask is not invoked
        // until this completionStage exits.
        try {
            lock.lock();
            if (shardSyncRequestPending.get()) {
                shardSyncRequestPending.set(false);
                // reset future to null, so next call creates a new one
                // without trying to get results from the old future.
                future = null;
                checkAndSubmitNextTask(null);
            }
        } finally {
            lock.unlock();
        }
    }
}