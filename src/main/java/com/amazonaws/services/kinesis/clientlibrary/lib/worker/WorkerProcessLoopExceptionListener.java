package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

/**
 * A listener for when the worker processing loop encounters an unexpected exception
 * so the client caller can inspect the error that occurred and take action.
 */
public interface WorkerProcessLoopExceptionListener {

    /**
     * Listeners implement this method and can read the exception to take the appropriate action.
     *
     * @param e the exception that occurred
     */
    void exceptionOccured(Exception e);
}
