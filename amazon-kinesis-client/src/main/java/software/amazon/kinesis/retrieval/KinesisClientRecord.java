/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.nio.ByteBuffer;
import java.time.Instant;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.awssdk.services.kinesis.model.Record;

/**
 * A converted record from Kinesis, maybe an aggregate record.
 */
@Builder(toBuilder = true)
@EqualsAndHashCode
@ToString
@Getter
@Accessors(fluent = true)
public class KinesisClientRecord {
    private final String sequenceNumber;
    private final Instant approximateArrivalTimestamp;
    private final ByteBuffer data;
    private final String partitionKey;
    private final EncryptionType encryptionType;
    private final long subSequenceNumber;
    private final String explicitHashKey;
    private final boolean aggregated;

    public static KinesisClientRecord fromRecord(Record record) {
        return KinesisClientRecord.builder().sequenceNumber(record.sequenceNumber())
                .approximateArrivalTimestamp(record.approximateArrivalTimestamp()).data(record.data().asByteBuffer())
                .partitionKey(record.partitionKey()).encryptionType(record.encryptionType()).build();
    }
}
