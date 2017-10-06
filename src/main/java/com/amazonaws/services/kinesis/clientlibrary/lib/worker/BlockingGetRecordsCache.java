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
    private final long idleMillisBetweenCalls;
    private Instant lastSuccessfulCall;

    public BlockingGetRecordsCache(final int maxRecordsPerCall,
                                   final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy,
                                   final long idleMillisBetweenCalls) {
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
        this.idleMillisBetweenCalls = idleMillisBetweenCalls;
    }

    @Override
    public void start() {
        //
        // Nothing to do here
        //
    }

    @Override
    public ProcessRecordsInput getNextResult() {
        sleepBeforeNextCall();
        GetRecordsResult getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
        lastSuccessfulCall = Instant.now();
        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(getRecordsResult.getRecords())
                .withMillisBehindLatest(getRecordsResult.getMillisBehindLatest());
        return processRecordsInput;
    }
    
    private void sleepBeforeNextCall() {
        if (!Thread.interrupted()) {
            if (lastSuccessfulCall == null) {
                return;
            }
            long timeSinceLastCall = Duration.between(lastSuccessfulCall, Instant.now()).abs().toMillis();
            if (timeSinceLastCall < idleMillisBetweenCalls) {
                try {
                    Thread.sleep(idleMillisBetweenCalls - timeSinceLastCall);
                } catch (InterruptedException e) {
                    log.info("Thread was interrupted, indicating that shutdown was called.");
                }
            }
        } else {
            log.info("Thread has been interrupted, indicating that it is in the shutdown phase.");
        }
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
