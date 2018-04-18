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

package software.amazon.kinesis.retrieval;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.model.GetRecordsResult;

import software.amazon.kinesis.lifecycle.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * This is the BlockingGetRecordsCache class. This class blocks any calls to the getRecords on the
 * GetRecordsRetrievalStrategy class.
 */
public class BlockingGetRecordsCache implements GetRecordsCache {
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;

    public BlockingGetRecordsCache(final int maxRecordsPerCall,
                                   final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy) {
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
    }

    @Override
    public void start(ExtendedSequenceNumber extendedSequenceNumber,
            InitialPositionInStreamExtended initialPositionInStreamExtended) {
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

    @Override
    public void addDataArrivedListener(DataArrivedListener dataArrivedListener) {

    }

    @Override
    public boolean hasResultAvailable() {
        return true;
    }
}
