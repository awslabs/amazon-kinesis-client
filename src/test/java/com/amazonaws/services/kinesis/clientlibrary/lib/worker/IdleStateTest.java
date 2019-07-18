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
public class IdleStateTest {

    private static final String WORKER_ID = "test_worker_id";

    @Mock
    WorkerStateChangeListener workerStateChangeListener;

    @Test
    public void testWorkerStateChangeListenerBehaviourOnLeaderStateTransition() {
        IdleState idleState = new IdleState(WORKER_ID, workerStateChangeListener);
        idleState.leaderStateTransition();
        verify(workerStateChangeListener, times(1)).startLeader();
        ArgumentCaptor<WorkerState> workerStateArgumentCaptor = ArgumentCaptor.forClass(WorkerState.class);
        verify(workerStateChangeListener, times(1)).setWorkerState(workerStateArgumentCaptor.capture());
        
        WorkerState actualWorkerState = workerStateArgumentCaptor.getValue();
        
        assertTrue("New WorkerState should be RunningState", actualWorkerState instanceof RunningState);
        assertEquals("Workerid doesn't match", WORKER_ID, actualWorkerState.workerId);
        assertEquals(workerStateChangeListener, actualWorkerState.workerStateChangeListener);
    }

    @Test
    public void testWorkerStateChangeListenerBehaviourOnNonLeaderStateTransition() {
        IdleState idleState = new IdleState(WORKER_ID, workerStateChangeListener);
        idleState.nonLeaderStateTransition();
        verify(workerStateChangeListener, times(1)).noOp();
        verify(workerStateChangeListener, times(0)).setWorkerState(any(WorkerState.class));
    }

    @Test
    public void testIsLeader() {
        IdleState idleState = new IdleState(WORKER_ID, workerStateChangeListener);
        boolean actualIsLeaderWorker = idleState
                .isLeaderWorker(new HashSet<String>(Arrays.asList("1", "2", WORKER_ID, "4")));
        assertTrue(String.format("%s is a leader", WORKER_ID), actualIsLeaderWorker);
    }

    @Test
    public void testIsLeaderWithEmptyNewLeaders() {
        IdleState idleState = new IdleState(WORKER_ID, workerStateChangeListener);
        boolean actualIsLeaderWorker = idleState
                .isLeaderWorker(new HashSet<String>());
        assertTrue(String.format("%s is not a leader", WORKER_ID), !actualIsLeaderWorker);
    }

    @Test
    public void testIsLeaderWithNullNewLeaders() {
        IdleState idleState = new IdleState(WORKER_ID, workerStateChangeListener);
        boolean actualIsLeaderWorker = idleState.isLeaderWorker(null);
        assertTrue(String.format("%s is not a leader", WORKER_ID), !actualIsLeaderWorker);
    }

    @Test
    public void testLeaderRefresh() {
        IdleState idleState = spy(new IdleState(WORKER_ID, workerStateChangeListener));
        idleState.refresh(new HashSet<String>(Arrays.asList("1", "2", WORKER_ID, "4")));
        verify(idleState, times(1)).leaderStateTransition();
        verify(idleState, times(0)).nonLeaderStateTransition();
    }

    @Test
    public void testNonLeaderRefresh() {
        IdleState idleState = spy(new IdleState(WORKER_ID, workerStateChangeListener));
        idleState.refresh(new HashSet<String>(Arrays.asList("1", "2", "3", "4")));
        verify(idleState, times(1)).nonLeaderStateTransition();
        verify(idleState, times(0)).leaderStateTransition();
    }
}
