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

import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

/**
 * This factory is used to create the records fetcher to retrieve data from Kinesis for a given shard.
 */
public interface RecordsFetcherFactory {
    /**
     * Returns a GetRecordsCache to be used for retrieving records for a given shard.
     *
     * @param getRecordsRetrievalStrategy GetRecordsRetrievalStrategy to be used with the GetRecordsCache
     * @param shardId ShardId of the shard that the fetcher will retrieve records for
     * @param metricsFactory MetricsFactory used to create metricScope
     *                
     * @return GetRecordsCache used to get records from Kinesis.
     */
    GetRecordsCache createRecordsFetcher(GetRecordsRetrievalStrategy getRecordsRetrievalStrategy, String shardId, IMetricsFactory metricsFactory);

    /**
     * Sets the maximum number of ProcessRecordsInput objects the GetRecordsCache can hold, before further requests are
     * blocked.
     * 
     * @param maxPendingProcessRecordsInput The maximum number of ProcessRecordsInput objects that the cache will accept
     *                                     before blocking.
     */
    void setMaxPendingProcessRecordsInput(int maxPendingProcessRecordsInput);

    /**
     * Sets the max byte size for the GetRecordsCache, before further requests are blocked. The byte size of the cache
     * is the sum of byte size of all the ProcessRecordsInput objects in the cache at any point of time.
     * 
     * @param maxByteSize The maximum byte size for the cache before blocking.
     */
    void setMaxByteSize(int maxByteSize);

    /**
     * Sets the max number of records for the GetRecordsCache can hold, before further requests are blocked. The records
     * count is the sum of all records present in across all the ProcessRecordsInput objects in the cache at any point
     * of time.
     * 
     * @param maxRecordsCount The mximum number of records in the cache before blocking.
     */
    void setMaxRecordsCount(int maxRecordsCount);

    /**
     * Sets the dataFetchingStrategy to determine the type of GetRecordsCache to be used.
     * 
     * @param dataFetchingStrategy Fetching strategy to be used
     */
    void setDataFetchingStrategy(DataFetchingStrategy dataFetchingStrategy);

    /**
     * Sets the maximum idle time between two get calls.
     * 
     * @param idleMillisBetweenCalls Sleep millis between calls.
     */
    void setIdleMillisBetweenCalls(long idleMillisBetweenCalls);

}
