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
package software.amazon.kinesis.multilang.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
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
    private Long approximateArrivalTimestamp;
    private Long subSequenceNumber;

    public static String ACTION = "record";

    public static JsonFriendlyRecord fromKinesisClientRecord(@NonNull final KinesisClientRecord record) {
        byte[] data;
        if (record.data() == null) {
            data = null;
        } else if (record.data().hasArray()) {
            data = record.data().array();
        } else {
            data = new byte[record.data().limit()];
            record.data().get(data);
        }
        Long approximateArrival = record.approximateArrivalTimestamp() == null
                ? null
                : record.approximateArrivalTimestamp().toEpochMilli();
        return new JsonFriendlyRecord(
                data, record.partitionKey(), record.sequenceNumber(), approximateArrival, record.subSequenceNumber());
    }

    @JsonProperty
    public String getAction() {
        return ACTION;
    }
}
