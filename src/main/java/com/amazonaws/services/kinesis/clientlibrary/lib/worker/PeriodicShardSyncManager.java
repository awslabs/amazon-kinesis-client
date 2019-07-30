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

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The top level orchestrator for coordinating the periodic shard sync related
 * activities.
 */
@Getter class PeriodicShardSyncManager {
    private static final Log LOG = LogFactory.getLog(PeriodicShardSyncManager.class);
    private static final long INITIAL_DELAY = 0;
    private static final long PERIODIC_SHARD_SYNC_INTERVAL_MILLIS = 1000;

    private final String workerId;
    private final LeaderDecider leaderDecider;
    private final ShardSyncTask shardSyncTask;
    private final ScheduledExecutorService shardSyncThreadPool;
    private boolean isRunning;

    private PeriodicShardSyncManager(PeriodicShardSyncManagerBuilder builder) {
        Validate.notBlank(builder.workerId, "WorkerID is required to initialize PeriodicShardSyncManager.");
        Validate.notNull(builder.leaderDecider, "LeaderDecider is required to initialize PeriodicShardSyncManager.");
        Validate.notNull(builder.shardSyncTask, "ShardSyncTask is required to initialize PeriodicShardSyncManager.");
        this.workerId = builder.workerId;
        this.leaderDecider = builder.leaderDecider;
        this.shardSyncTask = builder.shardSyncTask;
        if (builder.shardSyncThreadPool == null) {
            this.shardSyncThreadPool = Executors.newSingleThreadScheduledExecutor();
        } else {
            this.shardSyncThreadPool = builder.shardSyncThreadPool;
        }
    }

    static PeriodicShardSyncManagerBuilder getBuilder() {
        return new PeriodicShardSyncManagerBuilder();
    }

    public synchronized TaskResult start() {
        if (!isRunning) {
            shardSyncThreadPool
                    .scheduleWithFixedDelay(this::runShardSync, INITIAL_DELAY, PERIODIC_SHARD_SYNC_INTERVAL_MILLIS,
                            TimeUnit.MILLISECONDS);
            isRunning = true;
        }
        return new TaskResult(null);
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
        try {
            if (leaderDecider.isLeader(workerId)) {
                LOG.debug(String.format("WorkerId %s is a leader, running the shard sync task", workerId));
                shardSyncTask.run();
            } else {
                LOG.debug(String.format("WorkerId %s is not a leader, not running the shard sync task", workerId));
            }
        } catch (Throwable t) {
            LOG.error("Error during runShardSync.", t);
        }
    }

    @Override public int hashCode() {
        return Objects.hash(workerId);
    }

    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        PeriodicShardSyncManager other = (PeriodicShardSyncManager) obj;

        return Objects.equals(workerId, other.workerId);
    }

    static class PeriodicShardSyncManagerBuilder {
        private String workerId;
        private LeaderDecider leaderDecider;
        private ShardSyncTask shardSyncTask;
        private ScheduledExecutorService shardSyncThreadPool;

        private PeriodicShardSyncManagerBuilder() {
        }

        public PeriodicShardSyncManagerBuilder withWorkerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public PeriodicShardSyncManagerBuilder withLeaderDecider(LeaderDecider leaderDecider) {
            this.leaderDecider = leaderDecider;
            return this;
        }

        public PeriodicShardSyncManagerBuilder withShardSyncTask(ShardSyncTask shardSyncTask) {
            this.shardSyncTask = shardSyncTask;
            return this;
        }

        public PeriodicShardSyncManagerBuilder withShardSyncThreadPool(ScheduledExecutorService shardSyncThreadPool) {
            this.shardSyncThreadPool = shardSyncThreadPool;
            return this;
        }

        public PeriodicShardSyncManager build() {
            return new PeriodicShardSyncManager(this);
        }
    }
}
