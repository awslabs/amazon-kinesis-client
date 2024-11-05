/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Provides the lease assignment mode KCL must operate in during migration
 * from 2.x to 3.x.
 * KCL v2.x lease assignment is based on distributed-worker-stealing algorithm
 * which balances lease count across workers.
 * KCL v3.x lease assignment is based on a centralized-lease-assignment algorithm
 * which balances resource utilization metrics(e.g. CPU utilization) across workers.
 *
 * For a new application starting in KCL v3.x, there is no migration needed,
 * so KCL will initialize with the lease assignment mode accordingly, and it will
 * not change dynamically.
 *
 * During upgrade from 2.x to 3.x, KCL library needs an ability to
 * start in v2.x assignment mode but dynamically change to v3.x assignment.
 * In this case, both 2.x and 3.x lease assignment will be running but one
 * of them will be a no-op based on the mode.
 *
 * The methods and internal state is guarded for concurrent access to allow
 * both lease assignment algorithms to access the state concurrently while
 * it could be dynamically updated.
 */
@KinesisClientInternalApi
@Slf4j
@ThreadSafe
@NoArgsConstructor
public final class MigrationAdaptiveLeaseAssignmentModeProvider {

    public enum LeaseAssignmentMode {
        /**
         * This is the 2.x assignment mode.
         * This mode assigns leases based on the number of leases.
         * This mode involves each worker independently determining how many leases to pick or how many leases to steal
         * from other workers.
         */
        DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT,

        /**
         * This is the 3.x assigment mode.
         * This mode uses each worker's resource utilization to perform lease assignment.
         * Assignment is done by a single worker (elected leader), which looks at WorkerMetricStats for each worker to
         * determine lease assignment.
         *
         * This mode primarily does
         * 1. Starts WorkerMetricStatsManager on the worker which starts publishing WorkerMetricStats
         * 2. Starts the LeaseDiscoverer
         * 3. Creates if not already available the LeaseOwnerToLeaseKey GSI on the lease table and validate that is
         *    ACTIVE.
         */
        WORKER_UTILIZATION_AWARE_ASSIGNMENT;
    }

    private LeaseAssignmentMode currentMode;
    private boolean initialized = false;
    private boolean dynamicModeChangeSupportNeeded;

    /**
     * Specify whether both lease assignment algorithms should be initialized to
     * support dynamically changing lease mode.
     * @return true if lease assignment mode can change dynamically
     *         false otherwise.
     */
    public synchronized boolean dynamicModeChangeSupportNeeded() {
        return dynamicModeChangeSupportNeeded;
    }

    /**
     * Provide the current lease assignment mode in which KCL should perform lease assignment
     * @return  the current lease assignment mode
     */
    public synchronized LeaseAssignmentMode getLeaseAssignmentMode() {
        if (!initialized) {
            throw new IllegalStateException("AssignmentMode is not initialized");
        }
        return currentMode;
    }

    synchronized void initialize(final boolean dynamicModeChangeSupportNeeded, final LeaseAssignmentMode mode) {
        if (!initialized) {
            log.info("Initializing dynamicModeChangeSupportNeeded {} mode {}", dynamicModeChangeSupportNeeded, mode);
            this.dynamicModeChangeSupportNeeded = dynamicModeChangeSupportNeeded;
            this.currentMode = mode;
            this.initialized = true;
            return;
        }
        log.info(
                "Already initialized dynamicModeChangeSupportNeeded {} mode {}. Ignoring new values {}, {}",
                this.dynamicModeChangeSupportNeeded,
                this.currentMode,
                dynamicModeChangeSupportNeeded,
                mode);
    }

    synchronized void updateLeaseAssignmentMode(final LeaseAssignmentMode mode) {
        if (!initialized) {
            throw new IllegalStateException("Cannot change mode before initializing");
        }
        if (dynamicModeChangeSupportNeeded) {
            log.info("Changing Lease assignment mode from {} to {}", currentMode, mode);
            this.currentMode = mode;
            return;
        }
        throw new IllegalStateException(String.format(
                "Lease assignment mode already initialized to %s cannot" + " change to %s", this.currentMode, mode));
    }
}
