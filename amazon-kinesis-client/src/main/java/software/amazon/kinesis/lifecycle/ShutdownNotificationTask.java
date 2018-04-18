/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.processor.ShutdownNotificationAware;

/**
 * Notifies record processor of incoming shutdown request, and gives them a chance to checkpoint.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@Slf4j
public class ShutdownNotificationTask implements ITask {
    private final RecordProcessor recordProcessor;
    private final IRecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ShutdownNotification shutdownNotification;
//    TODO: remove if not used
    private final ShardInfo shardInfo;
    private TaskCompletedListener listener;

    @Override
    public TaskResult call() {
        try {
            if (recordProcessor instanceof ShutdownNotificationAware) {
                ShutdownNotificationAware shutdownNotificationAware = (ShutdownNotificationAware) recordProcessor;
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
    public TaskType taskType() {
        return TaskType.SHUTDOWN_NOTIFICATION;
    }

    @Override
    public void addTaskCompletedListener(TaskCompletedListener taskCompletedListener) {
        if (listener != null) {
            log.warn("Listener is being reset, this shouldn't happen");
        }
        listener = taskCompletedListener;
    }
}
