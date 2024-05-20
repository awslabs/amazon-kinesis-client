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

package software.amazon.kinesis.retrieval.polling;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.reactivestreams.Subscriber;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.RequestDetails;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * This is the BlockingRecordsPublisher class. This class blocks any calls to the records on the
 * GetRecordsRetrievalStrategy class.
 */
@KinesisClientInternalApi
public class BlockingRecordsPublisher implements RecordsPublisher {
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;

    private Subscriber<? super RecordsRetrieved> subscriber;
    private RequestDetails lastSuccessfulRequestDetails = new RequestDetails();

    public BlockingRecordsPublisher(
            final int maxRecordsPerCall, final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy) {
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
    }

    @Override
    public void start(
            ExtendedSequenceNumber extendedSequenceNumber,
            InitialPositionInStreamExtended initialPositionInStreamExtended) {
        //
        // Nothing to do here
        //
    }

    public ProcessRecordsInput getNextResult() {
        GetRecordsResponse getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
        final RequestDetails getRecordsRequestDetails = new RequestDetails(
                getRecordsResult.responseMetadata().requestId(), Instant.now().toString());
        setLastSuccessfulRequestDetails(getRecordsRequestDetails);
        List<KinesisClientRecord> records = getRecordsResult.records().stream()
                .map(KinesisClientRecord::fromRecord)
                .collect(Collectors.toList());
        return ProcessRecordsInput.builder()
                .records(records)
                .millisBehindLatest(getRecordsResult.millisBehindLatest())
                .childShards(getRecordsResult.childShards())
                .build();
    }

    @Override
    public void shutdown() {
        getRecordsRetrievalStrategy.shutdown();
    }

    private void setLastSuccessfulRequestDetails(RequestDetails requestDetails) {
        lastSuccessfulRequestDetails = requestDetails;
    }

    @Override
    public RequestDetails getLastSuccessfulRequestDetails() {
        return lastSuccessfulRequestDetails;
    }

    @Override
    public void subscribe(Subscriber<? super RecordsRetrieved> s) {
        subscriber = s;
    }

    @Override
    public void restartFrom(RecordsRetrieved recordsRetrieved) {
        throw new UnsupportedOperationException();
    }
}
