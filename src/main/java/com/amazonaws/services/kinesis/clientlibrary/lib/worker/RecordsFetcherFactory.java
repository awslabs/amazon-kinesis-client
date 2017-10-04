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

/**
 * The Amazon Kinesis Client Library will use this to instantiate a record fetcher per shard.
 * Clients may choose to create separate instantiations, or re-use instantiations.
 */

public interface RecordsFetcherFactory {

    /**
     * Returns a records fetcher processor to be used for processing data records for a (assigned) shard.
     *
     * @param getRecordsRetrievalStrategy GetRecordsRetrievalStrategy to be used with the GetRecordsCache
     * @param shardId ShardId of the shard for which the GetRecordsCache is to be returned
     *                
     * @return Returns a GetRecordsCache object
     */
    GetRecordsCache createRecordsFetcher(GetRecordsRetrievalStrategy getRecordsRetrievalStrategy, String shardId);

    /**
     * This method sets the maximum number of ProcessRecordsInput objects the GetRecordsCache can hold at any give time.
     * 
     * @param maxSize Max size for the cache.
     */
    void setMaxSize(int maxSize);

    /**
     * This method sets the max byte size for the GetRecordsCache. This is the sum of all the records bytes present in
     * the cache at a given point of time.
     * 
     * @param maxByteSize Maximum byte size for the cache.
     */
    void setMaxByteSize(int maxByteSize);

    /**
     * This method sets the max number of records for the GetRecordsCache. This is the sum of all the records present
     * across all the ProcessRecordsInput in the cache at a given point of time.
     * 
     * @param maxRecordsCount Maximum number of records in the cache.
     */
    void setMaxRecordsCount(int maxRecordsCount);

    /**
     * This method sets the dataFetchingStrategy to determine the type of GetRecordsCache to be used.
     * 
     * @param dataFetchingStrategy Fetching strategy to be used
     */
    void setDataFetchingStrategy(DataFetchingStrategy dataFetchingStrategy);

    /**
     * This method sets the maximum idle time between two get calls.
     * 
     * @param idleMillisBetweenCalls Sleep millis between calls.
     */
    void setIdleMillisBetweenCalls(long idleMillisBetweenCalls);

}
