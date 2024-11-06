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
     * Version the KCL needs to operate in. For more details check the KCLv3 migration
     * documentation.
     */
    public enum ClientVersionConfig {
        /**
         * For an application that was operating with previous KCLv2.x, during
         * upgrade to KCLv3.x, a migration process is needed due to the incompatible
         * changes between the 2 versions. During the migration process, application
         * must use ClientVersion=CLIENT_VERSION_COMPATIBLE_WITH_2x so that it runs in
         * a compatible mode until all workers in the cluster have upgraded to the version
         * running 3.x version (which is determined based on workers emitting WorkerMetricStats)
         * Once all known workers are in 3.x mode, the library auto toggles to 3.x mode;
         * but prior to that it runs in a mode compatible with 2.x workers.
         * This version also allows rolling back to the compatible mode from the
         * auto-toggled 3.x mode.
         */
        CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X,
        /**
         * A new application operating with KCLv3.x will use this value. Also, an application
         * that has successfully upgraded to 3.x version and no longer needs the ability
         * for a rollback to a 2.x compatible version, will use this value. In this version,
         * KCL will operate with new algorithms introduced in 3.x which is not compatible
         * with prior versions. And once in this version, rollback to 2.x is not supported.
         */
        CLIENT_VERSION_CONFIG_3X,
    }

    /**
     * Client version KCL must operate in, by default it operates in 3.x version which is not
     * compatible with prior versions.
     */
    private ClientVersionConfig clientVersionConfig = ClientVersionConfig.CLIENT_VERSION_CONFIG_3X;

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
