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
package software.amazon.kinesis.metrics;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.TaskType;

/**
 * Decorates an ConsumerTask and reports metrics about its timing and success/failure.
 */
@KinesisClientInternalApi
public class MetricsCollectingTaskDecorator implements ConsumerTask {

    private final ConsumerTask other;
    private final MetricsFactory factory;

    /**
     * Constructor.
     * 
     * @param other
     *            task to report metrics on
     * @param factory
     *            IMetricsFactory to use
     */
    public MetricsCollectingTaskDecorator(ConsumerTask other, MetricsFactory factory) {
        this.other = other;
        this.factory = factory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TaskResult call() {
        MetricsScope scope = MetricsUtil.createMetricsWithOperation(factory, other.getClass().getSimpleName());
        TaskResult result = null;
        final long startTimeMillis = System.currentTimeMillis();
        try {
            result = other.call();
        } finally {
            MetricsUtil.addSuccessAndLatency(scope, result != null && result.getException() == null, startTimeMillis,
                    MetricsLevel.SUMMARY);
            MetricsUtil.endScope(scope);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TaskType taskType() {
        return other.taskType();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "<" + other.taskType() + ">(" + other + ")";
    }

    public ConsumerTask getOther() {
        return other;
    }
}
