package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

public interface IShardConsumer {

    boolean isSkipShardSyncAtWorkerInitializationIfLeasesExist();

    enum TaskOutcome {
        SUCCESSFUL, END_OF_SHARD, NOT_COMPLETE, FAILURE, LEASE_NOT_FOUND
    }

    boolean consumeShard();

    boolean isShutdown();

    ShutdownReason getShutdownReason();

    boolean beginShutdown();

    void notifyShutdownRequested(ShutdownNotification shutdownNotification);

    KinesisConsumerStates.ShardConsumerState getCurrentState();

    boolean isShutdownRequested();
    
}
