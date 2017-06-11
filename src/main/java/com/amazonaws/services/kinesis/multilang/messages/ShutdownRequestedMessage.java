package com.amazonaws.services.kinesis.multilang.messages;

/**
 * A message to indicate to the client's process that shutdown is requested.
 */
public class ShutdownRequestedMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "shutdownRequested";

    /**
     * Convenience constructor.
     */
    public ShutdownRequestedMessage() {
    }
}
