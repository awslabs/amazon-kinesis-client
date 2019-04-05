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

import java.util.concurrent.ExecutorService;

import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.processor.Checkpointer;

/**
 * Used in the process of configuring and providing instances to the {@link Scheduler}
 */
public interface CoordinatorFactory {
    /**
     * Creates the executor service to be used by the Scheduler.
     *
     * @return ExecutorService
     */
    ExecutorService createExecutorService();

    /**
     * Creates GracefulShutdownCoordinator to be used by the Scheduler.
     *
     * <h3>Method Deprecated</h3>
     * <p>
     *     <strong>Note: This method has been deprecated, and will be removed in a future release. Use the configuration in
     *     {@link CoordinatorConfig#gracefulShutdownCoordinator}. Set the
     *     {@link CoordinatorConfig#gracefulShutdownCoordinator} to null in order to use this method.</strong>
     * </p>
     * <h4>Resolution Order</h3>
     * <ol>
     *     <li>{@link CoordinatorConfig#gracefulShutdownCoordinator()}</li>
     *     <li>{@link CoordinatorFactory#createGracefulShutdownCoordinator()}</li>
     * </ol>
     *
     *
     * @return a {@link GracefulShutdownCoordinator} that manages the process of shutting down the scheduler.
     */
    @Deprecated
    default GracefulShutdownCoordinator createGracefulShutdownCoordinator() {
        return new GracefulShutdownCoordinator();
    }

    /**
     * Creates a WorkerStateChangeListener to be used by the Scheduler.
     *
     * <h3>Method Deprecated</h3>
     * <p>
     *     <strong>Note: This method has been deprecated, and will be removed in a future release. Use the configuration in
     *     {@link CoordinatorConfig#workerStateChangeListener}. Set the
     *     {@link CoordinatorConfig#workerStateChangeListener} to null in order to use this method.</strong>
     * </p>
     *
     * <h4>Resolution Order</h3>
     * <ol>
     *     <li>{@link CoordinatorConfig#workerStateChangeListener()}</li>
     *     <li>{@link CoordinatorFactory#createWorkerStateChangeListener()}</li>
     * </ol>
     *
     * @return a {@link WorkerStateChangeListener} instance that will be notified for specific {@link Scheduler} steps.
     */
    @Deprecated
    default WorkerStateChangeListener createWorkerStateChangeListener() {
        return new NoOpWorkerStateChangeListener();
    }

    /**
     * Creates a RecordProcessorChedckpointer to be used by the Scheduler.
     *
     * @param shardInfo ShardInfo to be used in order to create the ShardRecordProcessorCheckpointer
     * @param checkpoint Checkpointer to be used in order to create Shardthe RecordProcessorCheckpointer
     * @return ShardRecordProcessorCheckpointer
     */
    ShardRecordProcessorCheckpointer createRecordProcessorCheckpointer(ShardInfo shardInfo, Checkpointer checkpoint);
}
