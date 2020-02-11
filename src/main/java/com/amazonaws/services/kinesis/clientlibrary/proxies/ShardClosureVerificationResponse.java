package com.amazonaws.services.kinesis.clientlibrary.proxies;

/**
 * ShutDown task verifies if the shard in context is necessarily closed before checkpointing the shard at SHARD_END.
 * The verification is done by IKinesisProxy and implementations of this interface are used to wrap the verification results.
 */
public interface ShardClosureVerificationResponse {

    /**
     * @return A boolean value indicating whether or not the shard in context (for which the request was made) is closed.
     */
    boolean isShardClosed();
}
