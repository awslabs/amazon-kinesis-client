/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.kinesis.retrieval.polling;

import lombok.Data;
import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalFactory;

import java.time.Duration;

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
    // private final long listShardsBackoffTimeInMillis;
    // private final int maxListShardsRetryAttempts;
    private final int maxRecords;
    private final Duration kinesisRequestTimeout;

    public SynchronousBlockingRetrievalFactory(String streamName, KinesisAsyncClient kinesisClient, RecordsFetcherFactory recordsFetcherFactory, int maxRecords, Duration kinesisRequestTimeout) {
        this.streamName = streamName;
        this.kinesisClient = kinesisClient;
        this.recordsFetcherFactory = recordsFetcherFactory;
        this.maxRecords = maxRecords;
        this.kinesisRequestTimeout = kinesisRequestTimeout;
    }

    @Deprecated
    public SynchronousBlockingRetrievalFactory(String streamName, KinesisAsyncClient kinesisClient, RecordsFetcherFactory recordsFetcherFactory, int maxRecords) {
        this(streamName, kinesisClient, recordsFetcherFactory, maxRecords, PollingConfig.DEFAULT_REQUEST_TIMEOUT);
    }

    @Override
    public GetRecordsRetrievalStrategy createGetRecordsRetrievalStrategy(@NonNull final ShardInfo shardInfo,
            @NonNull final MetricsFactory metricsFactory) {
        return new SynchronousGetRecordsRetrievalStrategy(
                new KinesisDataFetcher(kinesisClient, streamName, shardInfo.shardId(), maxRecords, metricsFactory, kinesisRequestTimeout));
    }

    @Override
    public RecordsPublisher createGetRecordsCache(@NonNull final ShardInfo shardInfo,
            @NonNull final MetricsFactory metricsFactory) {
        return recordsFetcherFactory.createRecordsFetcher(createGetRecordsRetrievalStrategy(shardInfo, metricsFactory),
                shardInfo.shardId(), metricsFactory, maxRecords);
    }
}
