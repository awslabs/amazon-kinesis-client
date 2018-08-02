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
package com.amazonaws.services.kinesis.multilang.messages;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * A message to indicate to the client's process that it should process a list of records.
 */
@Getter
@Setter
public class ProcessRecordsMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "processRecords";

    /**
     * The records that the client's process needs to handle.
     */
    private List<JsonFriendlyRecord> records;
    private Long millisBehindLatest;

    /**
     * Default constructor.
     */
    public ProcessRecordsMessage() {
    }

    /**
     * Convenience constructor.
     * 
     * @param processRecordsInput
     *            the process records input to be sent to the child
     */
    public ProcessRecordsMessage(ProcessRecordsInput processRecordsInput) {
        this.millisBehindLatest = processRecordsInput.millisBehindLatest();
        List<JsonFriendlyRecord> recordMessages = new ArrayList<>();
        for (KinesisClientRecord record : processRecordsInput.records()) {
            recordMessages.add(JsonFriendlyRecord.fromKinesisClientRecord(record));
        }
        this.setRecords(recordMessages);
    }
}
