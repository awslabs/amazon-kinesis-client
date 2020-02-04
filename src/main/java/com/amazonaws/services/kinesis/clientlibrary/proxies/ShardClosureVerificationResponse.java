package com.amazonaws.services.kinesis.clientlibrary.proxies;

import com.amazonaws.services.kinesis.model.Shard;
import lombok.Getter;

import java.util.List;

/**
 * ShutDown task verifies if the shard in context is necessarily closed before checkpointing the shard at SHARD_END.
 * The verification is done by IKinesisProxy and this class is used to wrap the verification results.
 */
@Getter
public class ShardClosureVerificationResponse {

    private final boolean verifiedShardWasClosed;
    private final List<Shard> latestShards;

    /**
     * Used to capture response from KinesisProxy.verifyShardClosure method.
     * @param verifiedShardWasClosed If the provided shardId corresponds to a shard that was verified as closed.
     * @param latestShards Result returned by KinesisProxy.getShardList() used for verification.
     */
    public ShardClosureVerificationResponse(boolean verifiedShardWasClosed, List<Shard> latestShards) {
        this.verifiedShardWasClosed = verifiedShardWasClosed;
        this.latestShards = latestShards;
    }
}
