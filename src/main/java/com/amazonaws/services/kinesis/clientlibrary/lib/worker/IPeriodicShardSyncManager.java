package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.google.common.annotations.VisibleForTesting;
import lombok.Value;
import lombok.experimental.Accessors;

public interface IPeriodicShardSyncManager {

    TaskResult start();

    /**
     * Runs ShardSync once, without scheduling further periodic ShardSyncs.
     * @return TaskResult from shard sync
     */
    TaskResult syncShardsOnce();

    void stop();

    @Value
    @Accessors(fluent = true)
    @VisibleForTesting
    class ShardSyncResponse {
        private final boolean shouldDoShardSync;
        private final boolean isHoleDetected;
        private final String reasonForDecision;
    }
}

