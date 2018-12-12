package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.List;

/**
 * Provides logic to prioritize or filter shards before their execution.
 */
public interface ShardPrioritization {

    /**
     * Returns new list of shards ordered based on their priority.
     * Resulted list may have fewer shards compared to original list
     * 
     * @param original
     *            list of shards needed to be prioritized
     * @return new list that contains only shards that should be processed
     */
    List<ShardInfo> prioritize(List<ShardInfo> original);
}
