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
package software.amazon.kinesis.coordinator.migration;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;

/**
 * Table State machine that provides:
 * 1. 2-Phase upgrade from 3.4 or lower to 3.5 or higher with a single DDB table for all KCL entity types
 *    such as Lease, CoordinatorState and WorkerMetricStats
 * 2. Backward compatible upgrade - 2 phase upgrade allows the migration to safely transition
 *    to a single table in Phase 2 when the Phase 1 code is forward compatible for non-lease entries
 *    in lease table.
 * 3. Support roll-back and roll-forward - rollback from Phase 2 to Phase 1 will revert back to using
 *    multiple DDB tables. Rollforward from Phase 1 will continue table migration.
 * 4. Table migration will complete in Phase 2 after a configured bake time.
 *
 * <p>During initialization, this state machine:</p>
 * <ol>
 *   <li>Calls {@code coordinatorStateDAO.initializeDelegates()} so reads work</li>
 *   <li>Determines the current {@link TableMigrationStatus} and sets the {@link TableMigrationStatusProvider}</li>
 *   <li>Calls {@code coordinatorStateDAO.initialize()} to enable writes</li>
 * </ol>
 *
 * <p>For 2→3 migration (where legacy tables never existed), this state machine
 * short-circuits to COMPLETE immediately.</p>
 *
 * <p>State transitions for 3.x→3.5 upgrade: INIT → DEPLOYED → PENDING → COMPLETE</p>
 * <ul>
 *   <li>INIT: Written by leader when entering CLIENT_VERSION_3X (3.4→3.5 upgrade)</li>
 *   <li>DEPLOYED: Leader moves here after monitoring min support code across fleet + bake time</li>
 *   <li>PENDING: Any worker moves here when Phase 2 config is detected</li>
 *   <li>COMPLETE: Leader moves here after bake time in PENDING, after copying all entities to lease table</li>
 * </ul>
 */
@KinesisClientInternalApi
public interface TableMigrationStateMachine {

    /**
     * Initialize the table migration state machine. This is called during KCL startup
     * before the client version migration state machine runs. It is idempotent — calling
     * it multiple times after successful initialization is a no-op.
     *
     * <p>This method:</p>
     * <ol>
     *   <li>Calls {@code coordinatorStateDAO.initializeDelegates()} — legacy checks table existence</li>
     *   <li>Determines TableMigrationStatus — for 2→3 (no legacy tables) → COMPLETE; for 3.x→3.5 reads from DDB</li>
     *   <li>Sets the {@link TableMigrationStatusProvider} with the determined status</li>
     *   <li>Calls {@code coordinatorStateDAO.initialize()} to enable write operations</li>
     * </ol>
     *
     * <p>If it fails, the Scheduler retries the entire initialization. On retry, if already
     * initialized, this is a no-op.</p>
     *
     * @throws DependencyException if DDB operations fail
     * @throws InvalidStateException if the state is inconsistent
     */
    void initialize() throws DependencyException, InvalidStateException;

    /**
     * Called after leader election to handle table migration state transitions that
     * require leader ownership. The leader drives transitions between states.
     *
     * <p>When the PENDING → COMPLETE transition completes (async copy done, status updated),
     * this method throws {@link InvalidStateException} to signal the caller to release the
     * current leader lock (held on the legacy table). The next {@code isLeader} check will
     * use the updated {@link TableMigrationStatusProvider} (now COMPLETE) to acquire the lock
     * from the lease table instead.</p>
     *
     * @param isLeader whether the current worker holds the leader lock
     * @throws DependencyException if DDB read/write fails — caller should treat as transient
     *         and retry on the next cycle (e.g., isLeader returns false for this cycle)
     * @throws InvalidStateException when the migration has just completed and the caller
     *         must release the current lock so the correct lock can be acquired
     */
    void handleLeaderLockResult(boolean isLeader) throws DependencyException, InvalidStateException;
}
