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

package software.amazon.kinesis.retrieval;

import org.reactivestreams.Publisher;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.RequestDetails;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Provides a record publisher that will retrieve records from Kinesis for processing
 */
public interface RecordsPublisher extends Publisher<RecordsRetrieved> {

    /**
     * Initializes the publisher with where to start processing. If there is a stored sequence number the publisher will
     * begin from that sequence number, otherwise it will use the initial position.
     *
     * @param extendedSequenceNumber
     *            the sequence number to start processing from
     * @param initialPositionInStreamExtended
     *            if there is no sequence number the initial position to use
     */
    void start(
            ExtendedSequenceNumber extendedSequenceNumber,
            InitialPositionInStreamExtended initialPositionInStreamExtended);

    /**
     * Restart from the last accepted and processed
     * @param recordsRetrieved the processRecordsInput to restart from
     */
    void restartFrom(RecordsRetrieved recordsRetrieved);

    /**
     * Shutdowns the publisher. Once this method returns the publisher should no longer provide any records.
     */
    void shutdown();

    /**
     * Gets last successful request details.
     *
     * @return details associated with last successful request.
     */
    RequestDetails getLastSuccessfulRequestDetails();

    /**
     * Notify the publisher on receipt of a data event.
     *
     * @param ack acknowledgement received from the subscriber.
     */
    default void notify(RecordsDeliveryAck ack) {
        throw new UnsupportedOperationException("RecordsPublisher does not support acknowledgement from Subscriber");
    }
}
