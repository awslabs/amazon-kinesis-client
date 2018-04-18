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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.Data;
import lombok.NonNull;
import software.amazon.kinesis.checkpoint.RecordProcessorCheckpointer;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;

/**
 *
 */
@Data
public class SchedulerCoordinatorFactory implements CoordinatorFactory {
    @Override
    public ExecutorService createExecutorService() {
        return new SchedulerThreadPoolExecutor(
                new ThreadFactoryBuilder().setNameFormat("RecordProcessor-%04d").build());
    }

    @Override
    public GracefulShutdownCoordinator createGracefulShutdownCoordinator() {
        return new GracefulShutdownCoordinator();
    }

    @Override
    public WorkerStateChangeListener createWorkerStateChangeListener() {
        return new NoOpWorkerStateChangeListener();
    }

    static class SchedulerThreadPoolExecutor extends ThreadPoolExecutor {
        private static final long DEFAULT_KEEP_ALIVE = 60L;
        SchedulerThreadPoolExecutor(ThreadFactory threadFactory) {
            super(0, Integer.MAX_VALUE, DEFAULT_KEEP_ALIVE, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    threadFactory);
        }
    }

    @Override
    public RecordProcessorCheckpointer createRecordProcessorCheckpointer(@NonNull final ShardInfo shardInfo,
                                                                         @NonNull final Checkpointer checkpoint,
                                                                         @NonNull final IMetricsFactory metricsFactory) {
        return new RecordProcessorCheckpointer(shardInfo, checkpoint, metricsFactory);
    }
}
