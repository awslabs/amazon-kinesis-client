package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.leases.impl.LeaseCleanupManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class KinesisShardConsumerFactory implements IShardConsumerFactory{

    @Override
    public IShardConsumer createShardConsumer(ShardInfo shardInfo,
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
                                              LeaseCleanupManager leaseCleanupManager) {
        return new KinesisShardConsumer(shardInfo,
                streamConfig,
                checkpointTracker,
                recordProcessor,
                recordProcessorCheckpointer,
                leaseCoordinator,
                parentShardPollIntervalMillis,
                cleanupLeasesUponShardCompletion,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                new KinesisDataFetcher(streamConfig.getStreamProxy(), shardInfo),
                retryGetRecordsInSeconds,
                maxGetRecordsThreadPool,
                config, shardSyncer, shardSyncStrategy,
                leaseCleanupManager);
    }
}
