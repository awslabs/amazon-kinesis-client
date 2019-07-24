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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderElectionStrategy;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScheduledLeaseLeaderPollerTest {
    @Mock
    private LeaderElectionStrategy<KinesisClientLease> leaderElectionStrategy;
    @Mock
    private KinesisClientLibConfiguration config;
    @Mock
    private ScheduledThreadPoolExecutor scheduledExecutor;

    @Test
    public void testLeaderPollingScheduled() {
        ScheduledLeaseLeaderPoller leaderPoller = new ScheduledLeaseLeaderPoller(leaderElectionStrategy, config,
                scheduledExecutor);
        leaderPoller.pollForLeaders();
        verify(scheduledExecutor, times(1)).scheduleWithFixedDelay(any(LeaderElectionStrategy.class), anyLong(), anyLong(),
                any(TimeUnit.class));
    }

    @Test
    public void testStopWithAwaitTerminationTrue() throws InterruptedException {
        ScheduledLeaseLeaderPoller leaderPoller = new ScheduledLeaseLeaderPoller(leaderElectionStrategy, config,
                scheduledExecutor);
        when(scheduledExecutor.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        leaderPoller.pollForLeaders();
        verify(scheduledExecutor, times(1)).scheduleWithFixedDelay(any(LeaderElectionStrategy.class), anyLong(),
                anyLong(), any(TimeUnit.class));
        leaderPoller.stop();
        verify(scheduledExecutor, times(1)).shutdown();
        verify(scheduledExecutor, times(0)).shutdownNow();
    }

    @Test
    public void testStopWithAwaitTerminationFalse() throws InterruptedException {
        ScheduledLeaseLeaderPoller leaderPoller = new ScheduledLeaseLeaderPoller(leaderElectionStrategy, config,
                scheduledExecutor);
        when(scheduledExecutor.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);
        leaderPoller.pollForLeaders();
        verify(scheduledExecutor, times(1)).scheduleWithFixedDelay(any(LeaderElectionStrategy.class), anyLong(),
                anyLong(), any(TimeUnit.class));
        leaderPoller.stop();
        verify(scheduledExecutor, times(1)).shutdown();
        verify(scheduledExecutor, times(1)).shutdownNow();
    }

}
