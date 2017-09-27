/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.Executors;

import lombok.extern.apachecommons.CommonsLog;

@CommonsLog
public class SimpleRecordsFetcherFactory implements RecordsFetcherFactory {
    private final int maxRecords;
    private int maxSize = 3;
    private int maxByteSize = 8 * 1024 * 1024;
    private int maxRecordsCount = 30000;
    private long idleMillisBetweenCalls = 1500L;
    private DataFetchingStrategy dataFetchingStrategy = DataFetchingStrategy.DEFAULT;

    public SimpleRecordsFetcherFactory(int maxRecords) {
        this.maxRecords = maxRecords;
    }

    @Override
    public GetRecordsCache createRecordsFetcher(GetRecordsRetrievalStrategy getRecordsRetrievalStrategy) {
        if(dataFetchingStrategy.equals(DataFetchingStrategy.DEFAULT)) {
            return new BlockingGetRecordsCache(maxRecords, getRecordsRetrievalStrategy, idleMillisBetweenCalls);
        } else {
            return new PrefetchGetRecordsCache(maxSize, maxByteSize, maxRecordsCount, maxRecords,
                    getRecordsRetrievalStrategy, Executors.newFixedThreadPool(1), idleMillisBetweenCalls);
        }
    }

    @Override
    public void setMaxSize(int maxSize){
        this.maxSize = maxSize;
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

    @Override
    public void setIdleMillisBetweenCalls(final long idleMillisBetweenCalls) {
        this.idleMillisBetweenCalls = idleMillisBetweenCalls;
    }
}
