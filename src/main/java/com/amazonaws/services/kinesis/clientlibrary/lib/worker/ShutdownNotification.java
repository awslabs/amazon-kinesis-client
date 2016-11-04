package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

/**
 * A shutdown request to the ShardConsumer
 */
public interface ShutdownNotification {
    /**
     * Used to indicate that the record processor has been notified of a requested shutdown, and given the chance to
     * checkpoint.
     *
     */
    void shutdownNotificationComplete();

    /**
     * Used to indicate that the record processor has completed the call to
     * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#shutdown(ShutdownInput)} has
     * completed.
     */
    void shutdownComplete();
}
