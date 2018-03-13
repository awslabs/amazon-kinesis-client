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

import java.util.Date;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

/**
 * Class for encoding Record objects to json. Needed because Records have byte buffers for their data field which causes
 * problems for the json library we're using.
 */
@Getter
@Setter
public class JsonFriendlyRecord {
    private byte[] data;
    private String partitionKey;
    private String sequenceNumber;
    private Date approximateArrivalTimestamp;
    private Long subSequenceNumber;

    public static String ACTION = "record";

    /**
     * Default Constructor.
     */
    public JsonFriendlyRecord() {
    }

    /**
     * Convenience constructor.
     *
     * @param record The record that this message will represent.
     */
    public JsonFriendlyRecord(Record record) {
        this.data = record.getData() == null ? null : record.getData().array();
        this.partitionKey = record.getPartitionKey();
        this.sequenceNumber = record.getSequenceNumber();
        this.approximateArrivalTimestamp = record.getApproximateArrivalTimestamp();
        if (record instanceof UserRecord) {
            this.subSequenceNumber = ((UserRecord) record).getSubSequenceNumber();
        } else {
            this.subSequenceNumber = null;
        }
    }

    @JsonProperty
    public String getAction() {
        return ACTION;
    }

}
