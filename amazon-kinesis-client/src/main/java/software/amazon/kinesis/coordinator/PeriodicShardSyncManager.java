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
package software.amazon.kinesis.coordinator;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.ShardSyncTask;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.metrics.MetricsCollectingTaskDecorator;
import software.amazon.kinesis.metrics.MetricsFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The top level orchestrator for coordinating the periodic shard sync related
 * activities.
 */
@Getter
@EqualsAndHashCode
@Slf4j
class PeriodicShardSyncManager {
    private static final long INITIAL_DELAY = 60 * 1000L;
    private static final long PERIODIC_SHARD_SYNC_INTERVAL_MILLIS = 5 * 60 * 1000L;

    private final String workerId;
    private final LeaderDecider leaderDecider;
    private final ConsumerTask metricsEmittingShardSyncTask;
    private final ScheduledExecutorService shardSyncThreadPool;
    private boolean isRunning;

    PeriodicShardSyncManager(String workerId, LeaderDecider leaderDecider, ShardSyncTask shardSyncTask, MetricsFactory metricsFactory) {
        this(workerId, leaderDecider, shardSyncTask, Executors.newSingleThreadScheduledExecutor(), metricsFactory);
    }

    PeriodicShardSyncManager(String workerId, LeaderDecider leaderDecider, ShardSyncTask shardSyncTask, ScheduledExecutorService shardSyncThreadPool, MetricsFactory metricsFactory) {
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
                    log.error("Error during runShardSync.", t);
                }
            };
            shardSyncThreadPool.scheduleWithFixedDelay(periodicShardSyncer, INITIAL_DELAY, PERIODIC_SHARD_SYNC_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
            isRunning = true;

        }
        return new TaskResult(null);
    }

    /**
     * Runs shardSync once
     * Does not schedule periodic shardSync
     * @return the result of the task
     */
    public synchronized TaskResult syncShardsOnce() {

        Exception lastException = null;
        try {
            if (!isRunning) {
                runShardSync();
            }
        } catch (Exception e) {
            lastException = e;
        }
        return new TaskResult(lastException);
    }

    public void stop() {
        if (isRunning) {
            log.info(String.format("Shutting down leader decider on worker %s", workerId));
            leaderDecider.shutdown();
            log.info(String.format("Shutting down periodic shard sync task scheduler on worker %s", workerId));
            shardSyncThreadPool.shutdown();
            isRunning = false;
        }
    }

    private void runShardSync() {
        if (leaderDecider.isLeader(workerId)) {
            log.info(String.format("WorkerId %s is a leader, running the shard sync task", workerId));
            final TaskResult taskResult = metricsEmittingShardSyncTask.call();
            if (taskResult != null && taskResult.getException() != null) {
                throw new KinesisClientLibIOException("Failed to sync shards", taskResult.getException());
            }
        } else {
            log.debug(String.format("WorkerId %s is not a leader, not running the shard sync task", workerId));
        }
    }

    /**
     * Checks if the entire hash range is covered
     * @return true if covered, false otherwise
     */
    public boolean hashRangeCovered() {
        // TODO: Implement method
        return true;
    }
}
