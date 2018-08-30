/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License. 
 */

package software.amazon.kinesis.coordinator;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.kinesis.leases.NoOpShardPrioritization;
import software.amazon.kinesis.leases.ShardPrioritization;

/**
 * Used by the KCL to configure the coordinator.
 */
@Data
@Accessors(fluent = true)
public class CoordinatorConfig {
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

}
