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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderPoller;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.TaskSchedulerStrategy;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeriodicShardSyncManagerTest {

    private static final String WORKER_ID = "test_worker_id";
    @Mock
    private IMetricsFactory metricsFactory;
    @Mock
    private TaskSchedulerStrategy taskSchedulerStrategy;
    @Mock
    private LeaderPoller<KinesisClientLease> leaderPoller;
    
    @Before
    public void setup() {
    }
    
    public PeriodicShardSyncManager getPeriodicShardSyncManager() {
        PeriodicShardSyncManager periodicShardSyncManager = PeriodicShardSyncManager.getBuilder().withLeaderPoller(leaderPoller).withMetricsFactory(metricsFactory)
                .withTaskSchedulerStrategy(taskSchedulerStrategy).withWorkerId(WORKER_ID).build();
        return periodicShardSyncManager;
    }

    @Test
    public void testPollingStarts() {
        PeriodicShardSyncManager periodicShardSyncManager = getPeriodicShardSyncManager();
        periodicShardSyncManager.start();
        assertTrue("isRunning should be true", periodicShardSyncManager.isRunning());
        verify(leaderPoller, times(1)).pollForLeaders();
    }

    @Test
    public void testPollingNotRestartedIfAlreadyRunnig() {
        PeriodicShardSyncManager periodicShardSyncManager = getPeriodicShardSyncManager();
        periodicShardSyncManager.start();
        assertTrue("isRunning should be true", periodicShardSyncManager.isRunning());
        verify(leaderPoller, times(1)).pollForLeaders();

        periodicShardSyncManager.start();
        assertTrue("isRunning should be true", periodicShardSyncManager.isRunning());
        // 1 accounts for the previous invocation
        verify(leaderPoller, times(1)).pollForLeaders();
    }

    @Test
    public void testPollerAndTaskSchedulerStop() {
        PeriodicShardSyncManager periodicShardSyncManager = getPeriodicShardSyncManager();
        periodicShardSyncManager.start();
        assertTrue("isRunning should be true", periodicShardSyncManager.isRunning());
        verify(leaderPoller, times(1)).pollForLeaders();

        periodicShardSyncManager.stop();
        assertTrue("isRunning should be false", !periodicShardSyncManager.isRunning());
        verify(leaderPoller, times(1)).stop();
        verify(taskSchedulerStrategy, times(1)).shutdown();
    }

    @Test
    public void testPollerAndTaskSchedulerNotStoppedIfShardSyncManagerNotAlreadyRunning() {
        PeriodicShardSyncManager periodicShardSyncManager = getPeriodicShardSyncManager();
        periodicShardSyncManager.stop();
        assertTrue("isRunning should be false", !periodicShardSyncManager.isRunning());
        verify(leaderPoller, times(0)).stop();
        verify(taskSchedulerStrategy, times(0)).stop();
    }

    @Test
    public void testStartLeaderUponLeaderElection() {
        PeriodicShardSyncManager periodicShardSyncManager = getPeriodicShardSyncManager();
        Set<String> newLeaders = new HashSet<String>(Arrays.asList("1", "2", WORKER_ID, "4"));
        periodicShardSyncManager.leadersElected(newLeaders);
        assertTrue("Running worker state expected", periodicShardSyncManager.getWorkerState() instanceof RunningState);
    }

    @Test
    public void testStopLeaderUponLeaderElection() {
        PeriodicShardSyncManager periodicShardSyncManager = getPeriodicShardSyncManager();
        Set<String> newLeaders = new HashSet<String>(Arrays.asList("1", "2", "3", "4"));
        periodicShardSyncManager.leadersElected(newLeaders);
        assertTrue("Idle worker state expected", periodicShardSyncManager.getWorkerState() instanceof IdleState);
    }

    @Test
    public void testStartLeaderStartsTaskScheduling() {
        PeriodicShardSyncManager periodicShardSyncManager = getPeriodicShardSyncManager();
        periodicShardSyncManager.startLeader();
        verify(taskSchedulerStrategy, times(1)).start();
    }

    @Test
    public void testStopLeaderStopsTaskScheduling() {
        PeriodicShardSyncManager periodicShardSyncManager = getPeriodicShardSyncManager();
        periodicShardSyncManager.startLeader();
        verify(taskSchedulerStrategy, times(1)).start();

        periodicShardSyncManager.stopLeader();
        verify(taskSchedulerStrategy, times(1)).stop();
    }

}
