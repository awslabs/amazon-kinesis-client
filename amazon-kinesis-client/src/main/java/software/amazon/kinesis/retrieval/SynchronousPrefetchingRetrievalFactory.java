/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package software.amazon.kinesis.retrieval;

import com.amazonaws.services.kinesis.AmazonKinesis;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.IMetricsFactory;

import java.util.concurrent.ExecutorService;

/**
 *
 */
@RequiredArgsConstructor
public class SynchronousPrefetchingRetrievalFactory implements RetrievalFactory {
    @NonNull
    private final String streamName;
    @NonNull
    private final AmazonKinesis amazonKinesis;
    private final int maxRecords;
    private final int maxPendingProcessRecordsInput;
    private final int maxByteSize;
    private final int maxRecordsCount;
    private final int maxRecordsPerCall;
    @NonNull
    private final ExecutorService executorService;
    private final long idleMillisBetweenCalls;
    @NonNull
    private final IMetricsFactory metricsFactory;



    @Override
    public GetRecordsRetrievalStrategy createGetRecordsRetrievalStrategy(final ShardInfo shardInfo) {
        return new SynchronousGetRecordsRetrievalStrategy(
                new KinesisDataFetcher(amazonKinesis, streamName, shardInfo.shardId(), maxRecords));
    }

    @Override
    public GetRecordsCache createGetRecordsCache(final ShardInfo shardInfo, final IMetricsFactory metricsFactory) {
        return new PrefetchGetRecordsCache(maxPendingProcessRecordsInput, maxByteSize, maxRecordsCount,
                maxRecordsPerCall, createGetRecordsRetrievalStrategy(shardInfo), executorService,
                idleMillisBetweenCalls, metricsFactory, "Prefetching", shardInfo.shardId());
    }
}
