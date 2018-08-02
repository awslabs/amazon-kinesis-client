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

import org.reactivestreams.Publisher;

import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Provides a record publisher that will retrieve records from Kinesis for processing
 */
public interface RecordsPublisher extends Publisher<ProcessRecordsInput> {
    /**
     * Initializes the publisher with where to start processing. If there is a stored sequence number the publisher will
     * begin from that sequence number, otherwise it will use the initial position.
     * 
     * @param extendedSequenceNumber
     *            the sequence number to start processing from
     * @param initialPositionInStreamExtended
     *            if there is no sequence number the initial position to use
     */
    void start(ExtendedSequenceNumber extendedSequenceNumber, InitialPositionInStreamExtended initialPositionInStreamExtended);
    

    /**
     * Shutdowns the publisher. Once this method returns the publisher should no longer provide any records.
     */
    void shutdown();
}
