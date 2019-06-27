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

import software.amazon.kinesis.leases.LeaseCoordinator;

import java.util.concurrent.ExecutorService;

/**
 * Holds state and emits {@link DiagnosticEvent}s for logging and visibility
 */
class DiagnosticEventFactory {
    private ExecutorService executorService;
    private LeaseCoordinator leaseCoordinator;

    DiagnosticEventFactory(ExecutorService executorService, LeaseCoordinator leaseCoordinator) {
        this.executorService = executorService;
        this.leaseCoordinator = leaseCoordinator;
    }

    ExecutorStateEvent emitExecutorStateEvent() {
        return new ExecutorStateEvent(executorService, leaseCoordinator);
    }

    RejectedTaskEvent emitRejectedTaskEvent(Throwable t) {
        return new RejectedTaskEvent(emitExecutorStateEvent(), t);
    }
}
