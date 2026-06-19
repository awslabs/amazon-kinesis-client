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

import java.util.Map;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorConfig;

/**
 * TableMigration support during upgrade from KCLv3.4 or lower to KCLv3.5 or higher.
 *
 * <p>Table migration follows a 2-phase deployment to consolidate CoordinatorState and
 * WorkerMetricStats from separate DDB tables into the Lease table:</p>
 * <ul>
 *   <li>Phase 1: Upgrade to KCLv3.5+ with default config ({@link CoordinatorConfig#migrateAllEntitiesToLeaseTable()}
 *       = false). Workers emit a min support code in WorkerMetricStats to signal readiness.</li>
 *   <li>Phase 2: Deploy KCLv3.5+ with {@link CoordinatorConfig#migrateAllEntitiesToLeaseTable()} = true.
 *       Workers switch to writing all entities (WorkerMetricStats, CoordinatorState) to the Lease table.</li>
 * </ul>
 *
 * <p>This enum is persisted in storage, so any changes to it need to be backward compatible.
 * Reorganizing the values is not backward compatible; if versions are removed, the corresponding
 * enum value cannot be reused without backward compatibility considerations.</p>
 *
 * <h2>DDB State Transitions (3.4x → 3.5x table migration):</h2>
 * <pre>
 *                                                  3.4x -> 3.5x Table Migration
 *   .------------------------------------------------------------------------------------------------------------------.
 *   | .-----------------------.-----------------------------.  .-------------------------------.                       |
 *   | |                       ^   min support code          |  |     code rollback             |                       |
 *   | |                       |   not met due to rollback   |  |                               |                       |
 *   | V                       |   (Best effort)             |  V                               |                       V
 * Start ------------------> INIT -----------------------> DEPLOYED -----------------------> PENDING ---------------> COMPLETE
 *        Phase 1                     Bake time                      Phase 2 deployment                Bake time
 *        deployment                                                 (migrateAllEntitiesToLeaseTable   (configured via
 *        Leader writes INIT                                          = true)                          tableMigrationCompleteBakeTimeSeconds)
 *        when all workers                                           Leader writes PENDING when
 *        emit min support code                                      all workers emit to lease table
 *        in WorkerMetricStats                                       and CoordinatorState entries
 *                                                                   are moved to Lease table
 *
 * </pre>
 *
 * <p>There is local state workers use and there is a global state that the leader manages
 * as a {@link TableMigrationState} which is a CoordinatorState entry in DDB.</p>
 *
 * @see TableMigrationStateMachineImpl
 * @see CoordinatorConfig#migrateAllEntitiesToLeaseTable()
 */
@KinesisClientInternalApi
public enum TableMigrationStatus {
    /**
     * This is a local state before table migration status is either
     * determined by reading the DDB status and the KCL configuration.
     */
    TABLE_MIGRATION_STATUS_UNKNOWN(0L),

    /**
     * This state in the DDB signifies all workers are emitting min support code in
     * WorkerMetricStats and the bake time to move to DEPLOYED has started.
     * Worker enter this state locally in phase1 deployment. In this local state
     * workers emit min support code in WorkerMetricStats which is written
     * to legacy table, but reads for WorkerMetricStats and CoordinatorState
     * will be merged from both legacy and lease table.
     */
    TABLE_MIGRATION_STATUS_INIT(0L),
    /**
     * This state in DDB signifies all workers are emitting min support code
     * in the Worker metric stats steadily for the bake time and the application
     * is ready to move on to phase 2 deployment.
     * Worker enter this state locally when the ddb state transitions to it.
     * In terms of functionality there is no difference, between INIT and DEPLOYED
     * workers continue to emit min support code in WorkerMetricStats into legacy table
     * and read WorkerMetricStats and CoordinatorState from both legacy and lease table.
     */
    TABLE_MIGRATION_STATUS_DEPLOYED(TableMigrationState.DEFAULT_BAKE_TIME_SECONDS),
    /**
     * This state in DDB signifies, all workers have been deployed with phase 2
     * cofiguration and have switched writing to workerMetricStats into lease table
     * and the leader has moved over all other coordinatorState entries from legacy
     * table to the lease table. This is when bake time to move to COMPLETE state starts.
     * Locally this state is entered when phase 2 code is deployed and this indicates
     * to workers to switch to writing all entities into lease table.
     * In this local state workers continue to emit min support code in WorkerMetricStats
     * written to lease table, and reads for WorkerMetricStats and CoordinatorState
     * will from both legacy and lease table.
     * This state allows rollback to DEPLOYED if the workers rollback to phase1
     * deployment and switch to writing entities to the corresponding table again.
     */
    TABLE_MIGRATION_STATUS_PENDING(0L),
    /**
     * This status indicates that table migration is complete. All workers will always use
     * the lease table for all entities for both reads and writes. There is no rollback
     * allowed once the status is COMPLETE. The bake time for this final step can be
     * configured using {@link CoordinatorConfig#tableMigrationCompleteBakeTimeSeconds()}.
     */
    TABLE_MIGRATION_STATUS_COMPLETE(TableMigrationState.DEFAULT_BAKE_TIME_SECONDS);

    private final Long defaultBakeTimeSecondsForTransition;

    TableMigrationStatus(Long defaultBakeTimeSecondsForTransition) {
        this.defaultBakeTimeSecondsForTransition = defaultBakeTimeSecondsForTransition;
    }

    public long getBakeTimeSeconds(Map<TableMigrationStatus, Long> overrides) {
        return overrides.getOrDefault(this, defaultBakeTimeSecondsForTransition);
    }
}
