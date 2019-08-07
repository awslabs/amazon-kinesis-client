package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

/**
 * Types of ShardSyncStrategy} implemented in KCL.
 */
public enum ShardSyncStrategyType {

    /* Shard sync are performed periodically */
    PERIODIC,
    /* Shard syncs are performed when processing of a shard completes */
    SHARD_END
}
