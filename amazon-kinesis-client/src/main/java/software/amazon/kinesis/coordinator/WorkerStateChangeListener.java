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
package software.amazon.kinesis.coordinator;

/**
 * A listener for callbacks on changes worker state
 */
@FunctionalInterface
public interface WorkerStateChangeListener {
    enum WorkerState {
        CREATED,
        INITIALIZING,
        STARTED,
        SHUT_DOWN_STARTED,
        SHUT_DOWN
    }

    void onWorkerStateChange(WorkerState newState);

    default void onAllInitializationAttemptsFailed(Throwable e) {}
}
