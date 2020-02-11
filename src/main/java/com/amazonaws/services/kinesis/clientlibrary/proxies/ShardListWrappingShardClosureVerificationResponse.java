package com.amazonaws.services.kinesis.clientlibrary.proxies;

import com.amazonaws.services.kinesis.model.Shard;
import lombok.Getter;

import java.util.List;

/**
 * Implementation of ShardClosureVerificationResponse that also wraps the latest results
 * from IKinesisProxy.getShardList() in it.
 */
public class ShardListWrappingShardClosureVerificationResponse implements ShardClosureVerificationResponse {

    private final boolean isShardClosed;

    @Getter
    private final List<Shard> latestShards;

    /**
     * Used to capture response from KinesisProxy.verifyShardClosure method.
     * @param isShardClosed If the provided shardId corresponds to a shard that was verified as closed.
     * @param latestShards Result returned by KinesisProxy.getShardList() used for verification.
     */
    public ShardListWrappingShardClosureVerificationResponse(boolean isShardClosed, List<Shard> latestShards) {
        this.isShardClosed = isShardClosed;
        this.latestShards = latestShards;
    }

    @Override public boolean isShardClosed() {
        return isShardClosed;
    }
}
