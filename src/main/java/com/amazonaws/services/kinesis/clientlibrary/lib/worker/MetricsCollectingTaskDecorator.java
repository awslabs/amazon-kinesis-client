/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

/**
 * Decorates an ITask and reports metrics about its timing and success/failure.
 */
class MetricsCollectingTaskDecorator implements ITask {

    private final ITask other;
    private IMetricsFactory factory;

    /**
     * Constructor.
     * 
     * @param other task to report metrics on
     * @param factory IMetricsFactory to use
     */
    public MetricsCollectingTaskDecorator(ITask other, IMetricsFactory factory) {
        this.other = other;
        this.factory = factory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TaskResult call() {
        String taskName = other.getClass().getSimpleName();
        MetricsHelper.startScope(factory, taskName);

        long startTimeMillis = System.currentTimeMillis();
        TaskResult result = other.call();

        MetricsHelper.addSuccessAndLatency(null, startTimeMillis, result.getException() == null);
        MetricsHelper.endScope();

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TaskType getTaskType() {
        return other.getTaskType();
    }

}
