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

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.LeaseCoordinator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

@Getter
@ToString(exclude = "isThreadPoolExecutor")
@Slf4j
@KinesisClientInternalApi
public class ExecutorStateEvent implements DiagnosticEvent {
    private static final String MESSAGE = "Current thread pool executor state: ";

    private boolean isThreadPoolExecutor;
    private String executorName;
    private int currentQueueSize;
    private int activeThreads;
    private int coreThreads;
    private int leasesOwned;
    private int largestPoolSize;
    private int maximumPoolSize;

    ExecutorStateEvent(ExecutorService executor, LeaseCoordinator leaseCoordinator) {
        this(executor);
        this.leasesOwned = leaseCoordinator.getAssignments().size();
    }

    public ExecutorStateEvent(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor) {
            this.isThreadPoolExecutor = true;

            ThreadPoolExecutor ex = (ThreadPoolExecutor) executor;
            this.executorName = ex.getClass().getSimpleName();
            this.currentQueueSize = ex.getQueue().size();
            this.activeThreads = ex.getActiveCount();
            this.coreThreads = ex.getCorePoolSize();
            this.largestPoolSize = ex.getLargestPoolSize();
            this.maximumPoolSize = ex.getMaximumPoolSize();
        }
    }

    @Override
    public void accept(DiagnosticEventHandler visitor) {
        // logging is only meaningful for a ThreadPoolExecutor executor service (default config)
        if (isThreadPoolExecutor) {
            visitor.visit(this);
        }
    }

    @Override
    public String message() {
        return MESSAGE + this.toString();
    }
}
