package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

/**
 * Types of ShardSyncStrategy} implemented in KCL.
 */
public enum ShardSyncStrategyType {

        PERIODIC, /* Shard sync are performed periodically */
        SHARD_END /* Shard syncs are performed when processing of a shard completes */

}
