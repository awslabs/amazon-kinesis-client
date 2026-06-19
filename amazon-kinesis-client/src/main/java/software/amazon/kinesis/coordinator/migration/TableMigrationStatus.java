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
 * Table migration follows a 2 phase migration.
 * Phase 1: Upgrade to KCLv3.5 where workers emit a SupportCode attribute in the WorkerMetricStats
 * Phase 2: Deploy KCLv3.5 with {@link CoordinateConfig.migrateAllEntitiesToLeaseTable = true}.
 * This starts off the table migration where the workers switch to using LeaseTable for all entities.
 *
 * This enum is persisted in storage, so any changes to it needs to be backward compatible.
 * Reorganizing the values is not backward compatible, also if versions are removed, the corresponding
 * enum value cannot be reused without backward compatibility considerations.
 *
 *                                                  2.x -> 3.5x Migration (go to complete)
 *   .-----------------------------------------------------------------------------------------------------------------.
 *   | .-----------------------.---------------------------------.  .---------------------------.                       |
 *   | |                       ^      min support code           |  |     code rollback         |                       |
 *   | |                       |      not met due to rollback    |  |                           |                       |
 *   | V                       |       (Best effort)             |  V                           |                       V
 * Start ------------------> INIT --------------------------> DEPLOYED ------------------> PENDING ---------------> COMPLETE
 *        Phase 1                     Bake time                         3.5 or higher                Bake time
 *        deployment                                                    Phase 2
 *        when all workers                                              deployment
 *        emit minSupportCode                                           (migrateAllEntitiesToLeaseTable
 *        in WorkerMetricStats                                          = true)
 *                                                                      When all workers emit
 *                                                                      WorkerMetricStats to lease
 *                                                                      and CoordinatorState entries
 *                                                                      are moved to Lease table
 * There is local state workers use and there is a global state that the leader manages
 * as a {@link TableMigrationState} which is a CoordinatorState in DDB table.
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
     * This version indicates that table migration is completed all workers will always use
     * lease table for all entities for both reads and writes and there is no rollback
     * allowed once the status is COMPLETE. The bake time for this final step can be
     * configured using {@link CoordinatorConfig.tableMigrationCompleteBakeTimeSeconds}
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
