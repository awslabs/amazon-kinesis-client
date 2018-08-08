/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.kinesis.retrieval.polling;

import java.util.List;
import java.util.stream.Collectors;

import org.reactivestreams.Subscriber;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;

import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * This is the BlockingRecordsPublisher class. This class blocks any calls to the records on the
 * GetRecordsRetrievalStrategy class.
 */
@KinesisClientInternalApi
public class BlockingRecordsPublisher implements RecordsPublisher {
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;

    private Subscriber<? super ProcessRecordsInput> subscriber;

    public BlockingRecordsPublisher(final int maxRecordsPerCall,
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

    public ProcessRecordsInput getNextResult() {
        GetRecordsResponse getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
        List<KinesisClientRecord> records = getRecordsResult.records().stream()
                .map(KinesisClientRecord::fromRecord).collect(Collectors.toList());
        return ProcessRecordsInput.builder()
                .records(records)
                .millisBehindLatest(getRecordsResult.millisBehindLatest())
                .build();
    }

    @Override
    public void shutdown() {
        getRecordsRetrievalStrategy.shutdown();
    }

    @Override
    public void subscribe(Subscriber<? super ProcessRecordsInput> s) {
        subscriber = s;
    }
}
