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

import lombok.Setter;
import lombok.extern.apachecommons.CommonsLog;

import java.util.concurrent.Executors;

@CommonsLog
public class SimpleRecordsFetcherFactory implements RecordsFetcherFactory {
    private final int maxRecords;
    private int maxSize = 10;
    private int maxByteSize = 15 * 1024 * 1024;
    private int maxRecordsCount = 30000;
    private DataFetchingStrategy dataFetchingStrategy = DataFetchingStrategy.DEFAULT;

    public SimpleRecordsFetcherFactory(int maxRecords) {
        this.maxRecords = maxRecords;
    }

    public SimpleRecordsFetcherFactory(int maxRecords, int maxSize, int maxByteSize, int maxRecordsCount) {
        this.maxRecords = maxRecords;
        this.maxSize = maxSize;
        this.maxByteSize = maxByteSize;
        this.maxRecordsCount = maxRecordsCount;
    }

    @Override
    public GetRecordsCache createRecordsFetcher(GetRecordsRetrievalStrategy getRecordsRetrievalStrategy) {
        if(dataFetchingStrategy.equals(DataFetchingStrategy.DEFAULT)) {
            return new BlockingGetRecordsCache(maxRecords, getRecordsRetrievalStrategy);
        } else {
            return new PrefetchGetRecordsCache(maxSize, maxByteSize, maxRecordsCount, maxRecords, dataFetchingStrategy,
                    getRecordsRetrievalStrategy, Executors.newFixedThreadPool(1));
        }
    }

    public void setMaxSize(int maxSize){
        this.maxSize = maxSize;
    }

    public void setMaxByteSize(int maxByteSize){
        this.maxByteSize = maxByteSize;
    }

    public void setDataFetchingStrategy(DataFetchingStrategy dataFetchingStrategy){
        this.dataFetchingStrategy = dataFetchingStrategy;
    }
}
