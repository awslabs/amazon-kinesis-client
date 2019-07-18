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
 * The worker in RunningState represents that the worker is executing the
 * periodic shard syncs.
 */
class RunningState extends WorkerState {

    RunningState(String workerId, WorkerStateChangeListener workerStateChangeListener) {
        this.workerId = workerId;
        this.workerStateChangeListener = workerStateChangeListener;
    }

    /*
     * RunningState worker shows up again as a leader, no action needed
     *
     * @see
     * com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.impl.
     * WorkerState#leaderStateTransition()
     */
    @Override
    protected void leaderStateTransition() {
        workerStateChangeListener.noOp();

    }

    /*
     * RunningState worker shows up as a non leader implies that the worker is
     * no longer a leader and it should stop the leader behaviour
     *
     * @see
     * com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.impl.
     * WorkerState#nonleaderStateTransition()
     */
    @Override
    protected void nonLeaderStateTransition() {
        workerStateChangeListener.stopLeader();
        workerStateChangeListener.setWorkerState(new IdleState(workerId, workerStateChangeListener));
    }

}
