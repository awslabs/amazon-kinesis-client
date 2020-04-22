/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.retrieval.polling;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.KinesisDataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalFactory;

/**
 *
 */
@KinesisClientInternalApi
public class SynchronousPrefetchingRetrievalFactory implements RetrievalFactory {
    @NonNull
    private final String streamName;
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final RecordsFetcherFactory recordsFetcherFactory;
    private final int maxRecords;
    @NonNull
    private final ExecutorService executorService;
    private final long idleMillisBetweenCalls;
    private final Duration maxFutureWait;

    @Deprecated
    public SynchronousPrefetchingRetrievalFactory(String streamName, KinesisAsyncClient kinesisClient,
            RecordsFetcherFactory recordsFetcherFactory, int maxRecords, ExecutorService executorService,
            long idleMillisBetweenCalls) {
        this(streamName, kinesisClient, recordsFetcherFactory, maxRecords, executorService, idleMillisBetweenCalls,
                PollingConfig.DEFAULT_REQUEST_TIMEOUT);
    }

    public SynchronousPrefetchingRetrievalFactory(String streamName, KinesisAsyncClient kinesisClient,
            RecordsFetcherFactory recordsFetcherFactory, int maxRecords, ExecutorService executorService,
            long idleMillisBetweenCalls, Duration maxFutureWait) {
        this.streamName = streamName;
        this.kinesisClient = kinesisClient;
        this.recordsFetcherFactory = recordsFetcherFactory;
        this.maxRecords = maxRecords;
        this.executorService = executorService;
        this.idleMillisBetweenCalls = idleMillisBetweenCalls;
        this.maxFutureWait = maxFutureWait;
    }

    @Override public GetRecordsRetrievalStrategy createGetRecordsRetrievalStrategy(@NonNull final ShardInfo shardInfo,
            @NonNull final MetricsFactory metricsFactory) {
        final StreamIdentifier streamIdentifier = shardInfo.streamIdentifierSerOpt().isPresent() ?
                StreamIdentifier.multiStreamInstance(shardInfo.streamIdentifierSerOpt().get()) :
                StreamIdentifier.singleStreamInstance(streamName);

        return new SynchronousGetRecordsRetrievalStrategy(
                new KinesisDataFetcher(kinesisClient, new KinesisDataFetcherProviderConfig(
                        streamIdentifier,
                        shardInfo.shardId(),
                        metricsFactory,
                        maxRecords,
                        maxFutureWait
                )));
    }

    @Override
    public RecordsPublisher createGetRecordsCache(@NonNull final ShardInfo shardInfo,
            @NonNull final MetricsFactory metricsFactory) {
        return new PrefetchRecordsPublisher(recordsFetcherFactory.maxPendingProcessRecordsInput(),
                recordsFetcherFactory.maxByteSize(), recordsFetcherFactory.maxRecordsCount(), maxRecords,
                createGetRecordsRetrievalStrategy(shardInfo, metricsFactory), executorService, idleMillisBetweenCalls,
                metricsFactory, "Prefetching", shardInfo.shardId());
    }
}
