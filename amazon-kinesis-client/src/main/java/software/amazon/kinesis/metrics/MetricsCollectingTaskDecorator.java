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
package software.amazon.kinesis.metrics;

import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.lifecycle.ITask;
import software.amazon.kinesis.lifecycle.TaskCompletedListener;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.TaskType;

/**
 * Decorates an ITask and reports metrics about its timing and success/failure.
 */
public class MetricsCollectingTaskDecorator implements ITask {

    private final ITask other;
    private final IMetricsFactory factory;
    private DelegateTaskCompletedListener delegateTaskCompletedListener;

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
        try {
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
        } finally {
            if (delegateTaskCompletedListener != null) {
                delegateTaskCompletedListener.dispatchIfNeeded();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TaskType taskType() {
        return other.taskType();
    }

    @Override
    public void addTaskCompletedListener(TaskCompletedListener taskCompletedListener) {
        delegateTaskCompletedListener = new DelegateTaskCompletedListener(taskCompletedListener);
        other.addTaskCompletedListener(delegateTaskCompletedListener);
    }

    @RequiredArgsConstructor
    private class DelegateTaskCompletedListener implements TaskCompletedListener {

        private final TaskCompletedListener delegate;
        private boolean taskCompleted = false;

        @Override
        public void taskCompleted(ITask task) {
            delegate.taskCompleted(task);
            taskCompleted = true;
        }

        public void dispatchIfNeeded() {
            if (!taskCompleted) {
                delegate.taskCompleted(MetricsCollectingTaskDecorator.this);
            }
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "<" + other.taskType() + ">(" + other + ")";
    }

    public ITask getOther() {
        return other;
    }
}
