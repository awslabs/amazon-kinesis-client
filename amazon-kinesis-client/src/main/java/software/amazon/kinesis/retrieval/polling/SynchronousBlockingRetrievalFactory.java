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
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.KinesisDataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalFactory;

/**
 *
 */
@Data
@KinesisClientInternalApi
public class SynchronousBlockingRetrievalFactory implements RetrievalFactory {

    @NonNull
    private final String streamName;
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final RecordsFetcherFactory recordsFetcherFactory;

    private final int maxRecords;
    private final Duration kinesisRequestTimeout;

    private final Function<DataFetcherProviderConfig, DataFetcher> dataFetcherProvider;

    public SynchronousBlockingRetrievalFactory(String streamName,
                                               KinesisAsyncClient kinesisClient,
                                               RecordsFetcherFactory recordsFetcherFactory,
                                               int maxRecords,
                                               Duration kinesisRequestTimeout,
                                               Function<DataFetcherProviderConfig, DataFetcher> dataFetcherProvider) {
        this.streamName = streamName;
        this.kinesisClient = kinesisClient;
        this.recordsFetcherFactory = recordsFetcherFactory;
        this.maxRecords = maxRecords;
        this.kinesisRequestTimeout = kinesisRequestTimeout;
        this.dataFetcherProvider = dataFetcherProvider == null ?
                defaultDataFetcherProvider(kinesisClient) : dataFetcherProvider;
    }

    private static Function<DataFetcherProviderConfig, DataFetcher> defaultDataFetcherProvider(
            KinesisAsyncClient kinesisClient) {
        return dataFetcherProviderConfig -> new KinesisDataFetcher(kinesisClient, dataFetcherProviderConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsPublisher createGetRecordsCache(@NonNull final ShardInfo shardInfo,
            @NonNull final MetricsFactory metricsFactory) {
        final DataFetcherProviderConfig kinesisDataFetcherProviderConfig = new KinesisDataFetcherProviderConfig(
                shardInfo.streamConfig().streamIdentifier(),
                shardInfo.shardId(),
                metricsFactory,
                maxRecords,
                kinesisRequestTimeout);

        final DataFetcher dataFetcher = this.dataFetcherProvider.apply(kinesisDataFetcherProviderConfig);

        return recordsFetcherFactory.createRecordsFetcher(new SynchronousGetRecordsRetrievalStrategy(dataFetcher),
                shardInfo.shardId(), metricsFactory, maxRecords);
    }
}
