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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.HashSet;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.WorkerStateChangeListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RunningStateTest {
    private static final String WORKER_ID = "test_worker_id";

    @Mock
    WorkerStateChangeListener workerStateChangeListener;

    @Test
    public void testWorkerStateChangeListenerBehaviourOnNonLeaderStateTransition() {
        RunningState runningState = new RunningState(WORKER_ID, workerStateChangeListener);
        runningState.nonLeaderStateTransition();
        verify(workerStateChangeListener, times(1)).stopLeader();
        ArgumentCaptor<WorkerState> workerStateArgumentCaptor = ArgumentCaptor.forClass(WorkerState.class);
        verify(workerStateChangeListener, times(1)).setWorkerState(workerStateArgumentCaptor.capture());

        WorkerState actualWorkerState = workerStateArgumentCaptor.getValue();

        assertTrue("New WorkerState should be IdleState", actualWorkerState instanceof IdleState);
        assertEquals("Workerid doesn't match", WORKER_ID, actualWorkerState.workerId);
        assertEquals(workerStateChangeListener, actualWorkerState.workerStateChangeListener);
    }

    @Test
    public void testWorkerStateChangeListenerBehaviourOnLeaderStateTransition() {
        RunningState runningState = new RunningState(WORKER_ID, workerStateChangeListener);
        runningState.leaderStateTransition();
        verify(workerStateChangeListener, times(1)).noOp();
        verify(workerStateChangeListener, times(0)).setWorkerState(any(WorkerState.class));
    }

    @Test
    public void testIsLeader() {
        RunningState runningState = new RunningState(WORKER_ID, workerStateChangeListener);
        boolean actualIsLeaderWorker = runningState
                .isLeaderWorker(new HashSet<String>(Arrays.asList("1", "2", WORKER_ID, "4")));
        assertTrue(String.format("%s is a leader", WORKER_ID), actualIsLeaderWorker);
    }

    @Test
    public void testIsLeaderWithEmptyNewLeaders() {
        RunningState runningState = new RunningState(WORKER_ID, workerStateChangeListener);
        boolean actualIsLeaderWorker = runningState.isLeaderWorker(new HashSet<String>());
        assertTrue(String.format("%s is not a leader", WORKER_ID), !actualIsLeaderWorker);
    }

    @Test
    public void testIsLeaderWithNullNewLeaders() {
        RunningState runningState = new RunningState(WORKER_ID, workerStateChangeListener);
        boolean actualIsLeaderWorker = runningState.isLeaderWorker(null);
        assertTrue(String.format("%s is not a leader", WORKER_ID), !actualIsLeaderWorker);
    }

    @Test
    public void testLeaderRefresh() {
        RunningState runningState = spy(new RunningState(WORKER_ID, workerStateChangeListener));
        runningState.refresh(new HashSet<String>(Arrays.asList("1", "2", WORKER_ID, "4")));
        verify(runningState, times(1)).leaderStateTransition();
        verify(runningState, times(0)).nonLeaderStateTransition();
    }

    @Test
    public void testNonLeaderRefresh() {
        RunningState runningState = spy(new RunningState(WORKER_ID, workerStateChangeListener));
        runningState.refresh(new HashSet<String>(Arrays.asList("1", "2", "3", "4")));
        verify(runningState, times(1)).nonLeaderStateTransition();
        verify(runningState, times(0)).leaderStateTransition();
    }
}
