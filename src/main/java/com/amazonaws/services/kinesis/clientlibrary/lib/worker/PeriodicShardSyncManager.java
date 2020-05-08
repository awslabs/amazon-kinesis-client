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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The top level orchestrator for coordinating the periodic shard sync related
 * activities.
 */
@Getter
@EqualsAndHashCode
class PeriodicShardSyncManager {
    private static final Log LOG = LogFactory.getLog(PeriodicShardSyncManager.class);
    private static final long INITIAL_DELAY = 0;
    private static final long PERIODIC_SHARD_SYNC_INTERVAL_MILLIS = 1000;

    private final String workerId;
    private final LeaderDecider leaderDecider;
    private final ITask metricsEmittingShardSyncTask;
    private final ScheduledExecutorService shardSyncThreadPool;
    private boolean isRunning;

    PeriodicShardSyncManager(String workerId, LeaderDecider leaderDecider, ShardSyncTask shardSyncTask, IMetricsFactory metricsFactory) {
       this(workerId, leaderDecider, shardSyncTask, Executors.newSingleThreadScheduledExecutor(), metricsFactory);
    }

    PeriodicShardSyncManager(String workerId, LeaderDecider leaderDecider, ShardSyncTask shardSyncTask, ScheduledExecutorService shardSyncThreadPool, IMetricsFactory metricsFactory) {
        Validate.notBlank(workerId, "WorkerID is required to initialize PeriodicShardSyncManager.");
        Validate.notNull(leaderDecider, "LeaderDecider is required to initialize PeriodicShardSyncManager.");
        Validate.notNull(shardSyncTask, "ShardSyncTask is required to initialize PeriodicShardSyncManager.");
        this.workerId = workerId;
        this.leaderDecider = leaderDecider;
        this.metricsEmittingShardSyncTask = new MetricsCollectingTaskDecorator(shardSyncTask, metricsFactory);
        this.shardSyncThreadPool = shardSyncThreadPool;
    }

    public synchronized TaskResult start() {
        if (!isRunning) {
            final Runnable periodicShardSyncer = () -> {
                try {
                    runShardSync();
                } catch (Throwable t) {
                    LOG.error("Error running shard sync.", t);
                }
            };

            shardSyncThreadPool
                    .scheduleWithFixedDelay(periodicShardSyncer, INITIAL_DELAY, PERIODIC_SHARD_SYNC_INTERVAL_MILLIS,
                            TimeUnit.MILLISECONDS);
            isRunning = true;
        }
        return new TaskResult(null);
    }

    /**
     * Runs ShardSync once, without scheduling further periodic ShardSyncs.
     * @return TaskResult from shard sync
     */
    public synchronized TaskResult syncShardsOnce() {
        LOG.info("Syncing shards once from worker " + workerId);
        return metricsEmittingShardSyncTask.call();
    }

    public void stop() {
        if (isRunning) {
            LOG.info(String.format("Shutting down leader decider on worker %s", workerId));
            leaderDecider.shutdown();
            LOG.info(String.format("Shutting down periodic shard sync task scheduler on worker %s", workerId));
            shardSyncThreadPool.shutdown();
            isRunning = false;
        }
    }

    private void runShardSync() {
        if (leaderDecider.isLeader(workerId)) {
            LOG.debug(String.format("WorkerId %s is a leader, running the shard sync task", workerId));
            metricsEmittingShardSyncTask.call();
        } else {
            LOG.debug(String.format("WorkerId %s is not a leader, not running the shard sync task", workerId));
        }

    }
}
