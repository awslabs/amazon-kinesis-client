package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.List;

/**
 * Shard Prioritization that returns the same original list of shards without any modifications.
 */
public class NoOpShardPrioritization implements
        ShardPrioritization {

    /**
     * Empty constructor for NoOp Shard Prioritization.
     */
    public NoOpShardPrioritization() {
    }

    @Override
    public List<ShardInfo> prioritize(List<ShardInfo> original) {
        return original;
    }
}
