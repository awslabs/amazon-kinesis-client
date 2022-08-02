package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.leases.impl.LeaseCleanupManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public interface IShardConsumerFactory {

    /**
     * Returns a shard consumer to be used for consuming a (assigned) shard.
     *
     * @return Returns a shard consumer object.
     */
    IShardConsumer createShardConsumer(ShardInfo shardInfo,
                                       StreamConfig streamConfig,
                                       ICheckpoint checkpointTracker,
                                       IRecordProcessor recordProcessor,
                                       RecordProcessorCheckpointer recordProcessorCheckpointer,
                                       KinesisClientLibLeaseCoordinator leaseCoordinator,
                                       long parentShardPollIntervalMillis,
                                       boolean cleanupLeasesUponShardCompletion,
                                       ExecutorService executorService,
                                       IMetricsFactory metricsFactory,
                                       long taskBackoffTimeMillis,
                                       boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
                                       Optional<Integer> retryGetRecordsInSeconds,
                                       Optional<Integer> maxGetRecordsThreadPool,
                                       KinesisClientLibConfiguration config, ShardSyncer shardSyncer, ShardSyncStrategy shardSyncStrategy,
                                       LeaseCleanupManager leaseCleanupManager);
}