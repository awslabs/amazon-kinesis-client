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
import java.util.Set;

import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import lombok.Getter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The top level orchestrator for coordinating the periodic shard sync related
 * activities like polling for leader election, scheduling shard sync taks,
 * responding to leader election events etc
 */
@Getter
class PeriodicShardSyncManager {

    private static final Log LOG = LogFactory.getLog(PeriodicShardSyncManager.class);

    private final String workerId;

    private final PeriodicShardSyncScheduler shardSyncScheduler;
    private final ScheduledLeaseLeaderPoller leaderPoller;
    private boolean isRunning;

    private PeriodicShardSyncManager(PeriodicShardSyncManagerBuilder builder) {
        this.workerId = builder.workerId;
        this.shardSyncScheduler = builder.shardSyncScheduler;
        this.leaderPoller = builder.leaderPoller;
    }

    static PeriodicShardSyncManagerBuilder getBuilder() {
        return new PeriodicShardSyncManagerBuilder();
    }

    static class PeriodicShardSyncManagerBuilder {
        private String workerId;
        private ScheduledLeaseLeaderPoller leaderPoller;
        private PeriodicShardSyncScheduler shardSyncScheduler;

        private PeriodicShardSyncManagerBuilder() {
        }

        public PeriodicShardSyncManagerBuilder withWorkerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public PeriodicShardSyncManagerBuilder withLeaderPoller(ScheduledLeaseLeaderPoller leaderPoller) {
            this.leaderPoller = leaderPoller;
            return this;
        }

        public PeriodicShardSyncManagerBuilder withPeriodicShardSyncScheduler(PeriodicShardSyncScheduler shardSyncScheduler) {
            this.shardSyncScheduler = shardSyncScheduler;
            return this;
        }

        public PeriodicShardSyncManager build() {
            return new PeriodicShardSyncManager(this);
        }
    }

    /*
     * Starts polling for leaders
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.
     * interfaces.IShardSyncManager#start()
     */
    public TaskResult start() {
        if (!isRunning) {
            leaderPoller.pollForLeaders();
            shardSyncScheduler.start();
            isRunning = true;
        }
        return new TaskResult(null);
    }

    public void stop() {
        if (isRunning) {
            LOG.info(String.format("Stopping the leader poller on the worker %s", workerId));
            leaderPoller.stop();
            LOG.info(String.format("Stopping the periodic shard sync task scheduler on the worker %s", workerId));
            shardSyncScheduler.shutdown();
            isRunning = false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        PeriodicShardSyncManager other = (PeriodicShardSyncManager) obj;

        return Objects.equals(workerId, other.workerId);
    }

}
