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

    ShutdownNotificationTask(IRecordProcessor recordProcessor, IRecordProcessorCheckpointer recordProcessorCheckpointer, ShutdownNotification shutdownNotification) {
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.shutdownNotification = shutdownNotification;
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
