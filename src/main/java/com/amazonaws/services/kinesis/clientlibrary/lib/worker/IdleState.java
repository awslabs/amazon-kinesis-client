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

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.WorkerStateChangeListener;

/**
 * The worker in IdleState represents that the worker is not executing the
 * periodic shard syncs.
 */
class IdleState extends WorkerState {

    IdleState(String workerId, WorkerStateChangeListener workerStateChangeListener) {
        this.workerId = workerId;
        this.workerStateChangeListener = workerStateChangeListener;
    }

    /*
     * IdleState worker on transitioning as a leader needs to start executing
     * the leader behaviour and update to the RunningState
     *
     * @see
     * com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.impl.
     * WorkerState#leaderStateTransition()
     */
    @Override
    protected void leaderStateTransition() {
        workerStateChangeListener.startLeader();
        workerStateChangeListener.setWorkerState(new RunningState(workerId, workerStateChangeListener));
    }

    /*
     * No operation needed if IdleState worker not elected as a leader
     *
     * @see
     * com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.impl.
     * WorkerState#nonleaderStateTransition()
     */
    @Override
    protected void nonLeaderStateTransition() {
        workerStateChangeListener.noOp();
    }

}
