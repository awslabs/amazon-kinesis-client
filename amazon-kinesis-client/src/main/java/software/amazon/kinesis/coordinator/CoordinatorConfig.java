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

import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.leases.NoOpShardPrioritization;
import software.amazon.kinesis.leases.ShardPrioritization;

/**
 * Used by the KCL to configure the coordinator.
 */
@Data
@Accessors(fluent = true)
public class CoordinatorConfig {

    private static final int PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT = 1;

    public CoordinatorConfig(final String applicationName) {
        this.applicationName = applicationName;
        this.coordinatorStateTableConfig = new CoordinatorStateTableConfig(applicationName);
    }

    /**
     * Application name used by checkpointer to checkpoint.
     *
     * @return String
     */
    @NonNull
    private final String applicationName;

    /**
     * The maximum number of attempts to initialize the Scheduler
     *
     * <p>Default value: 20</p>
     */
    private int maxInitializationAttempts = 20;

    /**
     * Interval in milliseconds between polling to check for parent shard completion.
     * Polling frequently will take up more DynamoDB IOPS (when there are leases for shards waiting on
     * completion of parent shards).
     *
     * <p>Default value: 10000L</p>
     */
    private long parentShardPollIntervalMillis = 10000L;

    /**
     * The Scheduler will skip shard sync during initialization if there are one or more leases in the lease table. This
     * assumes that the shards and leases are in-sync. This enables customers to choose faster startup times (e.g.
     * during incremental deployments of an application).
     *
     * <p>Default value: false</p>
     */
    private boolean skipShardSyncAtWorkerInitializationIfLeasesExist = false;

    /**
     * The number of milliseconds between polling of the shard consumer for triggering state changes, and health checks.
     *
     * <p>Default value: 1000 milliseconds</p>
     */
    private long shardConsumerDispatchPollIntervalMillis = 1000L;

    /**
     * Shard prioritization strategy.
     *
     * <p>Default value: {@link NoOpShardPrioritization}</p>
     */
    private ShardPrioritization shardPrioritization = new NoOpShardPrioritization();

    /**
     * WorkerStateChangeListener to be used by the Scheduler.
     *
     * <p>Default value: {@link NoOpWorkerStateChangeListener}</p>
     */
    private WorkerStateChangeListener workerStateChangeListener = new NoOpWorkerStateChangeListener();

    /**
     * GracefulShutdownCoordinator to be used by the Scheduler.
     *
     * <p>Default value: {@link GracefulShutdownCoordinator}</p>
     */
    private GracefulShutdownCoordinator gracefulShutdownCoordinator = new GracefulShutdownCoordinator();

    private CoordinatorFactory coordinatorFactory = new SchedulerCoordinatorFactory();

    /**
     * Interval in milliseconds between retrying the scheduler initialization.
     *
     * <p>Default value: 1000L</p>
     */
    private long schedulerInitializationBackoffTimeMillis = 1000L;

    /**
     * Version the KCL needs to operate in. For an application that was operating with
     * previous KCLv2.x, during upgrade to KCLv3.x, a migration process is needed due
     * to the incompatible changes between the 2 versions.
     *
     * The 2.x to 3.x migration uses a 3-phase deployment:
     * <ol>
     *   <li>Phase 1 ({@link #CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1}): Forward-compatible
     *       with non-lease entities being present in Lease table. Passive 2.x compatible
     *       mode.</li>
     *   <li>Phase 2 ({@link #CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X}): Starts the
     *       migration state machine. Stores additional KCL 3.x entities such as
     *       coordinator state, worker metrics etc in the Lease table. </li>
     *   <li>Phase 3 ({@link #CLIENT_VERSION_CONFIG_3X}): Full 3.x functionality. No rollback
     *       support. Terminal state.</li>
     * </ol>
     *
     * For more details check the KCLv3 migration documentation.
     */
    public enum ClientVersionConfig {
        /**
         * Phase 1 of 2.x to 3.x migration. In this phase, the worker operates in pure 2.x
         * compatible mode: deterministic leader election, lease-count-based assignment.
         * This phase primarily offers forward-compatibility to non-lease entities being
         * present in lease table.
         */
        CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1,
        /**
         * Phase 2 of 2.x to 3.x migration. In this phase, the worker starts the migration
         * state machine where it operates in a compatible mode to 2.x algorithms
         * until all workers in the cluster have upgraded to the version running Phase 2
         * code (where it emits WorkerMetricStats). Once all known workers are 3.x compatible,
         * the library auto toggles to 3.x mode functionality. This config allows rolling
         * back to 2.x compatible mode from the auto-toggled 3.x mode using a KCL Migration tool.
         */
        CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X,
        /**
         * A new application operating with KCLv3.x will use this value. Also, an application
         * that has successfully upgraded to 3.x version and no longer needs the ability
         * for a rollback to a 2.x compatible version, will use this value (i.e. phase 3 of the
         * migration from KCL version 2.x). In this version, KCL will operate with new algorithms
         * introduced in 3.x which is not compatible with prior versions. And once in this version,
         * rollback to 2.x is not supported.
         */
        CLIENT_VERSION_CONFIG_3X,
    }

    /**
     * Client version KCL must operate in, by default it operates in 3.x version which is not
     * compatible with prior versions.
     */
    private ClientVersionConfig clientVersionConfig = ClientVersionConfig.CLIENT_VERSION_CONFIG_3X;

    /**
     * Controls whether we should migrate all KCLv3 entities such as WorkerMetricStats and the
     * CoordinatorState into the lease table.
     * This is used for existing v3 applications running the legacy multi-table setup (separate
     * CoordinatorState and WorkerMetricStats tables).
     *
     * Migration follows a 2 phase deployment and is recommended to be done only after
     * ClientVersionConfig is in CLIENT_VERSION_CONFIG_3X to avoid performing 2 migrations at once.
     *
     * Phase 1 is just an upgrade to latest KCL version with the default value for this config.
     * This prepares for table migration. Phase 2 is a second deployement where this config
     * should be set to true. During second phase the workers start writing WorkerMetricStats
     * and CoordinatorState entries into Lease table.
     * Once all workers start writing exclusively to Lease table the leader allows for a
     * bake time of {@link tableMigrationCompleteBakeTimeSeconds}, after which the state machine
     * moves to a completed state. Ensure the bake time is sufficient to detect any regressions
     * and rollback.
     * After the state transitions to a COMPLETE state, rollbacks will not be possible and all
     * workers will only use lease table. A very large bake time also means two reads for every
     * read of a coordinator table.
     *
     * <p>This flag is only relevant for 3.5+ upgrades where source version has the legacy
     * multi-table setup. For 2.x → 3.x migrations client version migration, it will land
     * by default into a single table and no migration is needed and this flag will be ignored.</p>
     *
     * <p>Default value: false</p>
     */
    private boolean migrateAllEntitiesToLeaseTable = false;

    /**
     * Bake time in seconds before transitioning from PENDING to COMPLETE during table migration.
     * Once in PENDING state, the leader monitors that all worker metrics are actively being emitted
     * in the lease table and no active metrics remain in the legacy table. When this condition is
     * satisfied, the leader records a steadySinceEpoch (seconds) timestamp in DDB. Once the bake
     * time elapses from that steady timestamp, the leader transitions to COMPLETE.
     *
     * <p>If the steady condition is broken (e.g., a worker rolls back and starts writing to legacy),
     * the steadySinceEpoch resets and the bake time countdown starts over.</p>
     *
     * <p>Ensure this bake time is sufficient to detect regressions and rollback if needed.
     * Minimum: 1 hour. Maximum: 1 week.</p>
     *
     * <p>Default value: 86400 seconds (1 day)</p>
     */
    private long tableMigrationCompleteBakeTimeSeconds = TimeUnit.DAYS.toSeconds(1);

    private static final long TABLE_MIGRATION_COMPLETE_BAKE_TIME_MIN_SECONDS = TimeUnit.HOURS.toSeconds(1);
    private static final long TABLE_MIGRATION_COMPLETE_BAKE_TIME_MAX_SECONDS = TimeUnit.DAYS.toSeconds(7);

    /**
     * Returns the effective bake time in seconds, clamped between min (1 hour) and max (1 week).
     */
    public long effectiveTableMigrationCompleteBakeTimeSeconds() {
        return Math.max(
                TABLE_MIGRATION_COMPLETE_BAKE_TIME_MIN_SECONDS,
                Math.min(TABLE_MIGRATION_COMPLETE_BAKE_TIME_MAX_SECONDS, tableMigrationCompleteBakeTimeSeconds));
    }

    public static class CoordinatorStateTableConfig extends DdbTableConfig {
        private CoordinatorStateTableConfig(final String applicationName) {
            super(applicationName, "CoordinatorState");
        }
    }

    /**
     * Configuration to control how the CoordinatorState DDB table is created, such as table name,
     * billing mode, provisioned capacity. If no table name is specified, the table name will
     * default to applicationName-CoordinatorState. If no billing more is chosen, default is
     * On-Demand.
     */
    @NonNull
    private final CoordinatorStateTableConfig coordinatorStateTableConfig;
}
