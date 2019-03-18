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

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;

/**
 * Used to capture stream configuration and pass it along.
 */
class StreamConfig {

    private final IKinesisProxy streamProxy;
    private final int maxRecords;
    private final long idleTimeInMilliseconds;
    private final boolean callProcessRecordsEvenForEmptyRecordList;
    private InitialPositionInStreamExtended initialPositionInStream;
    private final boolean validateSequenceNumberBeforeCheckpointing;

    /**
     * @param proxy Used to fetch records and information about the stream
     * @param maxRecords Max records to be fetched in a call
     * @param idleTimeInMilliseconds Idle time between get calls to the stream
     * @param callProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
     *        GetRecords returned an empty record list.
     * @param validateSequenceNumberBeforeCheckpointing Whether to call Amazon Kinesis to validate sequence numbers
     * @param initialPositionInStream Initial position in stream
     */
    StreamConfig(IKinesisProxy proxy,
            int maxRecords,
            long idleTimeInMilliseconds,
            boolean callProcessRecordsEvenForEmptyRecordList,
            boolean validateSequenceNumberBeforeCheckpointing,
            InitialPositionInStreamExtended initialPositionInStream) {
        this.streamProxy = proxy;
        this.maxRecords = maxRecords;
        this.idleTimeInMilliseconds = idleTimeInMilliseconds;
        this.callProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList;
        this.validateSequenceNumberBeforeCheckpointing = validateSequenceNumberBeforeCheckpointing;
        this.initialPositionInStream = initialPositionInStream;
    }

    /**
     * @return the streamProxy
     */
    IKinesisProxy getStreamProxy() {
        return streamProxy;
    }

    /**
     * @return the maxRecords
     */
    int getMaxRecords() {
        return maxRecords;
    }

    /**
     * @return the idleTimeInMilliseconds
     */
    long getIdleTimeInMilliseconds() {
        return idleTimeInMilliseconds;
    }

    /**
     * @return the callProcessRecordsEvenForEmptyRecordList
     */
    boolean shouldCallProcessRecordsEvenForEmptyRecordList() {
        return callProcessRecordsEvenForEmptyRecordList;
    }

    /**
     * @return the initialPositionInStream
     */
    InitialPositionInStreamExtended getInitialPositionInStream() {
        return initialPositionInStream;
    }

    /**
     * @return validateSequenceNumberBeforeCheckpointing
     */
    boolean shouldValidateSequenceNumberBeforeCheckpointing() {
        return validateSequenceNumberBeforeCheckpointing;
    }
}
