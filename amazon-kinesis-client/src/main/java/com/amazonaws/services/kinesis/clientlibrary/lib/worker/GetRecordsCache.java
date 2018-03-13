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

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;

/**
 * This class is used as a cache for Prefetching data from Kinesis.
 */
public interface GetRecordsCache {
    /**
     * This method calls the start behavior on the cache, if available.
     */
    void start();
    
    /**
     * This method returns the next set of records from the Cache if present, or blocks the request till it gets the
     * next set of records back from Kinesis.
     * 
     * @return The next set of records.
     */
    ProcessRecordsInput getNextResult();
    
    GetRecordsRetrievalStrategy getGetRecordsRetrievalStrategy();

    /**
     * This method calls the shutdown behavior on the cache, if available.
     */
    void shutdown();
}
