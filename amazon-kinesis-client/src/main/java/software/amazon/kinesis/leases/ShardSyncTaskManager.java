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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import software.amazon.kinesis.common.InitialPositionInStreamExtended;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.coordinator.ExecutorStateEvent;
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
    private final boolean garbageCollectLeases;
    private final boolean ignoreUnexpectedChildShards;
    private final long shardSyncIdleTimeMillis;
    @NonNull
    private final ExecutorService executorService;
    @NonNull
    private final HierarchicalShardSyncer hierarchicalShardSyncer;
    @NonNull
    private final MetricsFactory metricsFactory;
    private ConsumerTask currentTask;
    private CompletableFuture<TaskResult> future;
    private AtomicBoolean shardSyncRequestPending;
    private final ReentrantLock lock;

    /**
     * Constructor.
     *
     * <p>NOTE: This constructor is deprecated and will be removed in a future release.</p>
     *
     * @param shardDetector
     * @param leaseRefresher
     * @param initialPositionInStream
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIdleTimeMillis
     * @param executorService
     * @param metricsFactory
     */
    @Deprecated
    public ShardSyncTaskManager(ShardDetector shardDetector, LeaseRefresher leaseRefresher,
            InitialPositionInStreamExtended initialPositionInStream, boolean cleanupLeasesUponShardCompletion,
            boolean ignoreUnexpectedChildShards, long shardSyncIdleTimeMillis, ExecutorService executorService,
            MetricsFactory metricsFactory) {
        this.shardDetector = shardDetector;
        this.leaseRefresher = leaseRefresher;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.garbageCollectLeases = true;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncIdleTimeMillis = shardSyncIdleTimeMillis;
        this.executorService = executorService;
        this.hierarchicalShardSyncer = new HierarchicalShardSyncer();
        this.metricsFactory = metricsFactory;
        this.shardSyncRequestPending = new AtomicBoolean(false);
        this.lock = new ReentrantLock();
    }

    /**
     * Constructor.
     *
     * @param shardDetector
     * @param leaseRefresher
     * @param initialPositionInStream
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIdleTimeMillis
     * @param executorService
     * @param hierarchicalShardSyncer
     * @param metricsFactory
     */
    public ShardSyncTaskManager(ShardDetector shardDetector, LeaseRefresher leaseRefresher,
            InitialPositionInStreamExtended initialPositionInStream, boolean cleanupLeasesUponShardCompletion,
            boolean ignoreUnexpectedChildShards, long shardSyncIdleTimeMillis, ExecutorService executorService,
            HierarchicalShardSyncer hierarchicalShardSyncer, MetricsFactory metricsFactory) {
        this.shardDetector = shardDetector;
        this.leaseRefresher = leaseRefresher;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.garbageCollectLeases = true;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncIdleTimeMillis = shardSyncIdleTimeMillis;
        this.executorService = executorService;
        this.hierarchicalShardSyncer = hierarchicalShardSyncer;
        this.metricsFactory = metricsFactory;
        this.shardSyncRequestPending = new AtomicBoolean(false);
        this.lock = new ReentrantLock();
    }

    /**
     * Call a ShardSyncTask and return the Task Result.
     * @return the Task Result.
     */
    public TaskResult callShardSyncTask() {
        final ShardSyncTask shardSyncTask = new ShardSyncTask(shardDetector,
                                               leaseRefresher,
                                               initialPositionInStream,
                                               cleanupLeasesUponShardCompletion,
                                               garbageCollectLeases,
                                               ignoreUnexpectedChildShards,
                                               shardSyncIdleTimeMillis,
                                               hierarchicalShardSyncer,
                                               metricsFactory);
        final ConsumerTask metricCollectingTask = new MetricsCollectingTaskDecorator(shardSyncTask, metricsFactory);
        return metricCollectingTask.call();
    }

    /**
     * Submit a ShardSyncTask and return if the submission is successful.
     * @return if the casting is successful.
     */
    public boolean submitShardSyncTask() {
        try {
            lock.lock();
            return checkAndSubmitNextTask();
        } finally {
            lock.unlock();
        }
    }

    private boolean checkAndSubmitNextTask() {
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
                                    garbageCollectLeases,
                                    ignoreUnexpectedChildShards,
                                    shardSyncIdleTimeMillis,
                                    hierarchicalShardSyncer,
                                    metricsFactory),
                            metricsFactory);
            future = CompletableFuture.supplyAsync(() -> currentTask.call(), executorService)
                                      .whenComplete((taskResult, exception) -> handlePendingShardSyncs(exception, taskResult));

            log.info(new ExecutorStateEvent(executorService).message());

            submittedNewTask = true;
            if (log.isDebugEnabled()) {
                log.debug("Submitted new {} task.", currentTask.taskType());
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Previous {} task still pending.  Not submitting new task. "
                          + "Triggered a pending request but will not be executed until the current request completes.", currentTask.taskType());
            }
            shardSyncRequestPending.compareAndSet(false /*expected*/, true /*update*/);
        }
        return submittedNewTask;
    }

    private void handlePendingShardSyncs(Throwable exception, TaskResult taskResult) {
        if (exception != null || taskResult.getException() != null) {
            log.error("Caught exception running {} task: {}", currentTask.taskType(),
                    exception != null ? exception : taskResult.getException());
        }
    }

}
