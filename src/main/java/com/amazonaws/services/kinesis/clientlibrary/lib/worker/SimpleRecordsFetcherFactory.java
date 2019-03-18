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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.Executors;

import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.apachecommons.CommonsLog;

@CommonsLog
public class SimpleRecordsFetcherFactory implements RecordsFetcherFactory {
    private int maxPendingProcessRecordsInput = 3;
    private int maxByteSize = 8 * 1024 * 1024;
    private int maxRecordsCount = 30000;
    private long idleMillisBetweenCalls = 1500L;
    private DataFetchingStrategy dataFetchingStrategy = DataFetchingStrategy.DEFAULT;
    
    @Override
    public GetRecordsCache createRecordsFetcher(GetRecordsRetrievalStrategy getRecordsRetrievalStrategy, String shardId,
                                                IMetricsFactory metricsFactory, int maxRecords) {
        if(dataFetchingStrategy.equals(DataFetchingStrategy.DEFAULT)) {
            return new BlockingGetRecordsCache(maxRecords, getRecordsRetrievalStrategy);
        } else {
            return new PrefetchGetRecordsCache(maxPendingProcessRecordsInput, maxByteSize, maxRecordsCount, maxRecords,
                    getRecordsRetrievalStrategy,
                    Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("prefetch-cache-" + shardId + "-%04d")
                            .build()),
                            idleMillisBetweenCalls,
                    metricsFactory,
                    "ProcessTask",
                    shardId);
        }
    }

    @Override
    public void setMaxPendingProcessRecordsInput(int maxPendingProcessRecordsInput){
        this.maxPendingProcessRecordsInput = maxPendingProcessRecordsInput;
    }

    @Override
    public void setMaxByteSize(int maxByteSize){
        this.maxByteSize = maxByteSize;
    }

    @Override
    public void setMaxRecordsCount(int maxRecordsCount) {
        this.maxRecordsCount = maxRecordsCount;
    }

    @Override
    public void setDataFetchingStrategy(DataFetchingStrategy dataFetchingStrategy){
        this.dataFetchingStrategy = dataFetchingStrategy;
    }

    public void setIdleMillisBetweenCalls(final long idleMillisBetweenCalls) {
        this.idleMillisBetweenCalls = idleMillisBetweenCalls;
    }
}
