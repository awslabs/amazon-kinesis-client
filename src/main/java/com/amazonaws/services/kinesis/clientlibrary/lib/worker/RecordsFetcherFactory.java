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
     * @return Returns a record fetcher object
     */
    GetRecordsCache createRecordsFetcher(GetRecordsRetrievalStrategy getRecordsRetrievalStrategy);

    void setMaxSize(int maxSize);

    void setMaxByteSize(int maxByteSize);

    void setMaxRecordsCount(int maxRecordsCount);

    void setDataFetchingStrategy(DataFetchingStrategy dataFetchingStrategy);
    
    void setIdleMillisBetweenCalls(long idleMillisBetweenCalls);

}
