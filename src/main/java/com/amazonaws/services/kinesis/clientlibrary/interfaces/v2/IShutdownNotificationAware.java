package com.amazonaws.services.kinesis.clientlibrary.interfaces.v2;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;

/**
 * Allows a record processor to indicate it's aware of requested shutdowns, and handle the request.
 */
public interface IShutdownNotificationAware {

    /**
     * Called when the worker has been requested to shutdown, and gives the record processor a chance to checkpoint.
     *
     * The record processor will still have shutdown called.
     * 
     * @param checkpointer the checkpointer that can be used to save progress.
     */
    void shutdownRequested(IRecordProcessorCheckpointer checkpointer);

}
