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

import java.util.Set;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.WorkerStateChangeListener;
import com.amazonaws.util.CollectionUtils;

/**
 * Concrete WorkerState implementations represent the current state the worker
 * is in. Each state has its own behaviour for leader/non leader state
 * transition
 */
public abstract class WorkerState {
    protected String workerId;
    protected WorkerStateChangeListener workerStateChangeListener;

    /**
     * @param newLeaders current list of leaders
     * @return returns <code>true</code> if the worker is one of the leaders else <code>false</code>
     */
    protected boolean isLeaderWorker(Set<String> newLeaders) {
        if(CollectionUtils.isNullOrEmpty(newLeaders) || !newLeaders.contains(workerId)) {
            return false;
        }
        return true;
    }

    /**
     * Refreshes the state based on the worker being a leader or not
     *
     * @param newLeaders
     *            current list of leaders
     */
    protected void refresh(Set<String> newLeaders) {
        if (isLeaderWorker(newLeaders)) {
            leaderStateTransition();
            return;
        }
        nonLeaderStateTransition();
    }

    protected abstract void leaderStateTransition();

    protected abstract void nonLeaderStateTransition();
}
