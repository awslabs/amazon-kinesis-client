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

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * Class for encoding Record objects to json. Needed because Records have byte buffers for their data field which causes
 * problems for the json library we're using.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class JsonFriendlyRecord {
    private byte[] data;
    private String partitionKey;
    private String sequenceNumber;
    private Instant approximateArrivalTimestamp;
    private Long subSequenceNumber;

    public static String ACTION = "record";

    public static JsonFriendlyRecord fromKinesisClientRecord(@NonNull final KinesisClientRecord record) {
        byte[] data = record.data() == null ? null : record.data().array();
        return new JsonFriendlyRecord(data, record.partitionKey(), record.sequenceNumber(),
                record.approximateArrivalTimestamp(), record.subSequenceNumber());
    }

    @JsonProperty
    public String getAction() {
        return ACTION;
    }
}
