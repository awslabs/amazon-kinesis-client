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

import java.time.Duration;
import java.time.Instant;

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.GetRecordsResult;

import lombok.extern.apachecommons.CommonsLog;

/**
 * This is the BlockingGetRecordsCache class. This class blocks any calls to the getRecords on the
 * GetRecordsRetrievalStrategy class.
 */
@CommonsLog
public class BlockingGetRecordsCache implements GetRecordsCache {
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;

    public BlockingGetRecordsCache(final int maxRecordsPerCall,
                                   final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy) {
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
    }

    @Override
    public void start() {
        //
        // Nothing to do here
        //
    }

    @Override
    public ProcessRecordsInput getNextResult() {
        GetRecordsResult getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
        return new ProcessRecordsInput()
                .withRecords(getRecordsResult.getRecords())
                .withMillisBehindLatest(getRecordsResult.getMillisBehindLatest());
    }
    
    @Override
    public GetRecordsRetrievalStrategy getGetRecordsRetrievalStrategy() {
        return getRecordsRetrievalStrategy;
    }

    @Override
    public void shutdown() {
        getRecordsRetrievalStrategy.shutdown();
    }
}
