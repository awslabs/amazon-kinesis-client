/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    private InitialPositionInStream initialPositionInStream;

    /**
     * @param proxy Used to fetch records and information about the stream
     * @param maxRecords Max records to fetch in a call
     * @param idleTimeInMilliseconds Idle time between get calls to the stream
     * @param callProcessRecordsEvenForEmptyRecordList Call the RecordProcessor::processRecords() API even if
     *        GetRecords returned an empty record list.
     */
    StreamConfig(IKinesisProxy proxy,
            int maxRecords,
            long idleTimeInMilliseconds,
            boolean callProcessRecordsEvenForEmptyRecordList) {
        this(proxy, maxRecords, idleTimeInMilliseconds, callProcessRecordsEvenForEmptyRecordList,
                InitialPositionInStream.LATEST);
    }

    /**
     * @param proxy Used to fetch records and information about the stream
     * @param maxRecords Max records to be fetched in a call
     * @param idleTimeInMilliseconds Idle time between get calls to the stream
     * @param callProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
     *        GetRecords returned an empty record list.
     * @param initialPositionInStream Initial position in stream
     */
    StreamConfig(IKinesisProxy proxy,
            int maxRecords,
            long idleTimeInMilliseconds,
            boolean callProcessRecordsEvenForEmptyRecordList,
            InitialPositionInStream initialPositionInStream) {
        this.streamProxy = proxy;
        this.maxRecords = maxRecords;
        this.idleTimeInMilliseconds = idleTimeInMilliseconds;
        this.callProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList;
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
    InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }

}
