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

import com.amazonaws.services.kinesis.model.GetRecordsResult;

import java.util.Objects;

/**
 *
 */
public class SynchronousGetRecordsRetrievalStrategy implements GetRecordsRetrievalStrategy {
    private final KinesisDataFetcher dataFetcher;

    public SynchronousGetRecordsRetrievalStrategy(KinesisDataFetcher dataFetcher) {
        this.dataFetcher = dataFetcher;
        Objects.requireNonNull(dataFetcher);
    }

    @Override
    public GetRecordsResult getRecords(final int maxRecords) {
        return dataFetcher.getRecords(maxRecords).accept();
    }

    @Override
    public void shutdown() {
        //
        // Does nothing as this retriever doesn't manage any resources
        //
    }

    @Override
    public boolean isShutdown() {
        return false;
    }
    
    @Override
    public KinesisDataFetcher getDataFetcher() {
        return dataFetcher;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SynchronousGetRecordsRetrievalStrategy that = (SynchronousGetRecordsRetrievalStrategy) o;
        return Objects.equals(dataFetcher, that.dataFetcher);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataFetcher);
    }

    @Override
    public String toString() {
        return "SynchronousGetRecordsRetrievalStrategy{" +
                "dataFetcher=" + dataFetcher +
                '}';
    }
}
