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
 *   |                        .---------------------------------.  .---------------------------.                       |
 *   |                        |      min support code           |  |     code rollback         |                       |
 *   |                        |      not met due to rollback    |  |                           |                       |
 *   |                        V       (Best effort)             |  V                           |                       V
 * Start ------------------> INIT --------------------------> DEPLOYED ------------------> PENDING ---------------> COMPLETE
 *        3.4 or lower             MinSupport                            3.5 or higher                Bake time
 *        to 3.5 or higher         Code supports                         Phase 2
 *        Phase 1                  Single table                          deployment
 *        deployment               (migrateAllEntitiesToLeaseTable       (migrateAllEntitiesToLeaseTable
 *        if MigrationState           = default/false)                      = true)
 *          .ClientVersion         + Bake time
 *          = CLIENT_VERSION_3X
 *
 * When PENDING state is entered
 */
@KinesisClientInternalApi
public enum TableMigrationStatus {
    /**
     * This is a local state before table migration status is either
     * determined by reading the DDB status and the KCL configuration.
     */
    TABLE_MIGRATION_STATUS_UNKNOWN(null),

    /**
     * This is the starting version, set during the upgrade to KCLv3.5 Phase1. In this version KCL workers
     * will emit an additional SupportCode attribute in WorkerMetricStats.
     */
    TABLE_MIGRATION_STATUS_INIT(null),
    /**
     * The KCL fleet leader will monitor for the minimum support code from WorkerMetricStats
     * across the fleet and when it supports the Table Migration for a specific bake time, it transitions
     * to the DEPLOYED status to indicate completion of phase 1.
     */
    TABLE_MIGRATION_STATUS_DEPLOYED(TableMigrationState.DEFAULT_BAKE_TIME_SECONDS),
    /**
     * This version is used to initiate Phase 2 which allows workers to start using LeaseTable for all
     * entities. This state transition is triggered by the fleet leader when 2 conditions are met:
     * 1. All workers have Phase2 deployment which means they are emitted WorkerMetricsStats into LeaseTable
     * 2. Leader successfully copied over all CoordinatorState from Legacy table to LeaseTable.
     * Once these 2 conditions are met, leader transitions to PENDING state.
     * This state allows rollback to DEPLOYED if there were to be any regressions.
     */
    TABLE_MIGRATION_STATUS_PENDING(null),
    /**
     * This version indicates that table migration is completed all workers will always use lease table
     * for all entities for both reads and writes and there is no rollback allowed once the
     * status is COMPLETE. The bake time for this final step can be configured
     * using {@link CoordinatorConfig.tableMigrationCompleteBakeTimeSeconds}
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
