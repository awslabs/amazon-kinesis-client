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

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.IShardSyncManager;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderPoller;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeadersElectionListener;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.TaskSchedulerStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.WorkerStateChangeListener;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import lombok.Getter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The top level orchestrator for coordinating the periodic shard sync related
 * activities like polling for leader election, scheduling shard sync taks,
 * responding to leader election events etc
 */
@Getter
class PeriodicShardSyncManager
        implements IShardSyncManager<KinesisClientLease>, LeadersElectionListener, WorkerStateChangeListener {

    private static final Log LOG = LogFactory.getLog(PeriodicShardSyncManager.class);

    private final String workerId;
    private final IMetricsFactory metricsFactory;

    private final TaskSchedulerStrategy taskSchedulerStrategy;
    private final LeaderPoller<KinesisClientLease> leaderPoller;
    private com.amazonaws.services.kinesis.clientlibrary.lib.worker.WorkerState workerState;
    private boolean isRunning;

    private PeriodicShardSyncManager(PeriodicShardSyncManagerBuilder builder) {
        this.workerId = builder.workerId;
        this.metricsFactory = builder.metricsFactory;

        this.taskSchedulerStrategy = builder.taskSchedulerStrategy;
        this.leaderPoller = builder.leaderPoller;
        this.workerState = new IdleState(workerId, this);
    }

    static PeriodicShardSyncManagerBuilder getBuilder() {
        return new PeriodicShardSyncManagerBuilder();
    }

    static class PeriodicShardSyncManagerBuilder {
        private String workerId;
        private IMetricsFactory metricsFactory;

        private TaskSchedulerStrategy taskSchedulerStrategy;
        private LeaderPoller<KinesisClientLease> leaderPoller;

        private PeriodicShardSyncManagerBuilder() {
        }

        public PeriodicShardSyncManagerBuilder withTaskSchedulerStrategy(TaskSchedulerStrategy taskSchedulerStrategy) {
            this.taskSchedulerStrategy = taskSchedulerStrategy;
            return this;
        }

        public PeriodicShardSyncManagerBuilder withLeaderPoller(LeaderPoller<KinesisClientLease> leaderPoller) {
            this.leaderPoller = leaderPoller;
            return this;
        }

        public PeriodicShardSyncManagerBuilder withWorkerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public PeriodicShardSyncManagerBuilder withMetricsFactory(IMetricsFactory metricsFactory) {
            this.metricsFactory = metricsFactory;
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
    @Override
    public void start() {
        if (!isRunning) {
            leaderPoller.pollForLeaders();
            isRunning = true;
        }
    }

    /*
     * callback for being notified about the new leaders election. Refreshes
     * worker state upon being notified
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.
     * interfaces.LeadersElectionListener#leadersElected(java.util.Set)
     */
    @Override
    public synchronized void leadersElected(Set<String> newLeadersWorkerIds) {
        LOG.info(String.format("Worker %s notified about new leaders %s", workerId, newLeadersWorkerIds));
        workerState.refresh(newLeadersWorkerIds);
    }

    /*
     * Inititate Leader action, in this case start periodic shard syncs
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.
     * interfaces.WorkerStateChangeListener#startLeader()
     */
    @Override
    public void startLeader() {
        LOG.info(String.format("Worker %s is one of the elected leaders, starting periodic shard syncs ", workerId));
        taskSchedulerStrategy.start();
    }

    /*
     * Stop Leader action, in this case stop periodic shard syncs
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.
     * interfaces.WorkerStateChangeListener#stopLeader()
     */
    @Override
    public void stopLeader() {
        LOG.info(String.format("Worker %s is no longer an elected leader, stopping periodic shard syncs ", workerId));
        taskSchedulerStrategy.stop();
    }

    @Override
    public void noOp() {
        LOG.info(String.format("Worker %s, no change in worker state", workerId));
    }

    @Override
    public void setWorkerState(com.amazonaws.services.kinesis.clientlibrary.lib.worker.WorkerState workerState) {
        this.workerState = workerState;
    }

    @Override
    public LeaderPoller<KinesisClientLease> getLeaderPoller() {
        return leaderPoller;
    }

    @Override
    public void stop() {
        if (isRunning) {
            LOG.info(String.format("Stopping the leader poller on the worker %s", workerId));
            leaderPoller.stop();
            LOG.info(String.format("Stopping the periodic shard sync task scheduler on the worker %s", workerId));
            taskSchedulerStrategy.shutdown();
            workerState = new IdleState(workerId, this);
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
