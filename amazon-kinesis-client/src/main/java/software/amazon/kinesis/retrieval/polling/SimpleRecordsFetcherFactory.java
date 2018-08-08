/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package software.amazon.kinesis.retrieval.polling;

import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.DataFetchingStrategy;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;

@Slf4j
@KinesisClientInternalApi
public class SimpleRecordsFetcherFactory implements RecordsFetcherFactory {
    private int maxPendingProcessRecordsInput = 3;
    private int maxByteSize = 8 * 1024 * 1024;
    private int maxRecordsCount = 30000;
    private long idleMillisBetweenCalls = 1500L;
    private DataFetchingStrategy dataFetchingStrategy = DataFetchingStrategy.DEFAULT;

    @Override
    public RecordsPublisher createRecordsFetcher(GetRecordsRetrievalStrategy getRecordsRetrievalStrategy, String shardId,
                                                 MetricsFactory metricsFactory, int maxRecords) {

        return new PrefetchRecordsPublisher(maxPendingProcessRecordsInput, maxByteSize, maxRecordsCount, maxRecords,
                getRecordsRetrievalStrategy,
                Executors
                        .newFixedThreadPool(1,
                                new ThreadFactoryBuilder().setDaemon(true)
                                        .setNameFormat("prefetch-cache-" + shardId + "-%04d").build()),
                idleMillisBetweenCalls, metricsFactory, "ProcessTask", shardId);

    }

    @Override
    public void maxPendingProcessRecordsInput(int maxPendingProcessRecordsInput){
        this.maxPendingProcessRecordsInput = maxPendingProcessRecordsInput;
    }

    @Override
    public void maxByteSize(int maxByteSize){
        this.maxByteSize = maxByteSize;
    }

    @Override
    public void maxRecordsCount(int maxRecordsCount) {
        this.maxRecordsCount = maxRecordsCount;
    }

    @Override
    public void dataFetchingStrategy(DataFetchingStrategy dataFetchingStrategy){
        this.dataFetchingStrategy = dataFetchingStrategy;
    }

    @Override
    public void idleMillisBetweenCalls(final long idleMillisBetweenCalls) {
        this.idleMillisBetweenCalls = idleMillisBetweenCalls;
    }

    @Override
    public int maxPendingProcessRecordsInput() {
        return maxPendingProcessRecordsInput;
    }

    @Override
    public int maxByteSize() {
        return maxByteSize;
    }

    @Override
    public int maxRecordsCount() {
        return maxRecordsCount;
    }

    @Override
    public DataFetchingStrategy dataFetchingStrategy() {
        return dataFetchingStrategy;
    }

    @Override
    public long idleMillisBetweenCalls() {
        return idleMillisBetweenCalls;
    }
}
