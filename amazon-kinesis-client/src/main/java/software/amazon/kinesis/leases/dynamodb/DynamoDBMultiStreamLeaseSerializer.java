/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.leases.dynamodb;

import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.kinesis.leases.DynamoUtils;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.MultiStreamLease;

import java.util.Map;

import static software.amazon.kinesis.leases.MultiStreamLease.validateAndCast;

@NoArgsConstructor
public class DynamoDBMultiStreamLeaseSerializer extends DynamoDBLeaseSerializer {

    // Keeping the stream id as "streamName" for legacy reasons.
    private static final String STREAM_ID_KEY = "streamName";
    private static final String SHARD_ID_KEY = "shardId";

    @Override
    public Map<String, AttributeValue> toDynamoRecord(Lease lease) {
        final MultiStreamLease multiStreamLease = validateAndCast(lease);
        final Map<String, AttributeValue> result = super.toDynamoRecord(multiStreamLease);
        result.put(STREAM_ID_KEY, DynamoUtils.createAttributeValue(multiStreamLease.streamIdentifier()));
        result.put(SHARD_ID_KEY, DynamoUtils.createAttributeValue(multiStreamLease.shardId()));
        return result;
    }

    @Override
    public MultiStreamLease fromDynamoRecord(Map<String, AttributeValue> dynamoRecord) {
        final MultiStreamLease multiStreamLease = (MultiStreamLease) super
                .fromDynamoRecord(dynamoRecord, new MultiStreamLease());
        multiStreamLease.streamIdentifier(DynamoUtils.safeGetString(dynamoRecord, STREAM_ID_KEY));
        multiStreamLease.shardId(DynamoUtils.safeGetString(dynamoRecord, SHARD_ID_KEY));
        return multiStreamLease;
    }


    @Override
    public Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(Lease lease) {
        final MultiStreamLease multiStreamLease = validateAndCast(lease);
        final Map<String, AttributeValueUpdate> result = super.getDynamoUpdateLeaseUpdate(multiStreamLease);
        result.put(STREAM_ID_KEY, putUpdate(DynamoUtils.createAttributeValue(multiStreamLease.streamIdentifier())));
        result.put(SHARD_ID_KEY, putUpdate(DynamoUtils.createAttributeValue(multiStreamLease.shardId())));
        return result;
    }
}
