/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

/**
 * Decorates an ITask and reports metrics about its timing and success/failure.
 */
class MetricsCollectingTaskDecorator implements ITask {

    private final ITask other;
    private final IMetricsFactory factory;

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
        MetricsHelper.startScope(factory, other.getClass().getSimpleName());
        TaskResult result = null;
        final long startTimeMillis = System.currentTimeMillis();
        try {
            result = other.call();
        } finally {
            MetricsHelper.addSuccessAndLatency(startTimeMillis, result != null && result.getException() == null,
                    MetricsLevel.SUMMARY);
            MetricsHelper.endScope();
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TaskType getTaskType() {
        return other.getTaskType();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "<" + other.getTaskType() + ">(" + other + ")";
    }

    ITask getOther() {
        return other;
    }
}
