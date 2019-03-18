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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;

/**
 * Notifies record processor of incoming shutdown request, and gives them a chance to checkpoint.
 */
class ShutdownNotificationTask implements ITask {

    private final IRecordProcessor recordProcessor;
    private final IRecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ShutdownNotification shutdownNotification;
    private final ShardInfo shardInfo;

    ShutdownNotificationTask(IRecordProcessor recordProcessor, IRecordProcessorCheckpointer recordProcessorCheckpointer, ShutdownNotification shutdownNotification, ShardInfo shardInfo) {
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.shutdownNotification = shutdownNotification;
        this.shardInfo = shardInfo;
    }

    @Override
    public TaskResult call() {
        try {
            if (recordProcessor instanceof IShutdownNotificationAware) {
                IShutdownNotificationAware shutdownNotificationAware = (IShutdownNotificationAware) recordProcessor;
                try {
                    shutdownNotificationAware.shutdownRequested(recordProcessorCheckpointer);
                } catch (Exception ex) {
                    return new TaskResult(ex);
                }
            }
            return new TaskResult(null);
        } finally {
            shutdownNotification.shutdownNotificationComplete();
        }
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.SHUTDOWN_NOTIFICATION;
    }
}
