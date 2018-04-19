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
package software.amazon.kinesis.leases;

import java.util.Collection;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import com.google.common.base.Strings;

/**
 * An implementation of ILeaseSerializer for KinesisClientLease objects.
 */
public class KinesisClientLeaseSerializer implements LeaseSerializer<KinesisClientLease> {

    private static final String OWNER_SWITCHES_KEY = "ownerSwitchesSinceCheckpoint";
    private static final String CHECKPOINT_SEQUENCE_NUMBER_KEY = "checkpoint";
    private static final String CHECKPOINT_SUBSEQUENCE_NUMBER_KEY = "checkpointSubSequenceNumber";
    private static final String PENDING_CHECKPOINT_SEQUENCE_KEY = "pendingCheckpoint";
    private static final String PENDING_CHECKPOINT_SUBSEQUENCE_KEY = "pendingCheckpointSubSequenceNumber";
    public final String PARENT_SHARD_ID_KEY = "parentShardId";

    private final DynamoDBLeaseSerializer baseSerializer = new DynamoDBLeaseSerializer(KinesisClientLease.class);

    @Override
    public Map<String, AttributeValue> toDynamoRecord(KinesisClientLease lease) {
        Map<String, AttributeValue> result = baseSerializer.toDynamoRecord(lease);

        result.put(OWNER_SWITCHES_KEY, DynamoUtils.createAttributeValue(lease.getOwnerSwitchesSinceCheckpoint()));
        result.put(CHECKPOINT_SEQUENCE_NUMBER_KEY, DynamoUtils.createAttributeValue(lease.getCheckpoint().getSequenceNumber()));
        result.put(CHECKPOINT_SUBSEQUENCE_NUMBER_KEY, DynamoUtils.createAttributeValue(lease.getCheckpoint().getSubSequenceNumber()));
        if (lease.getParentShardIds() != null && !lease.getParentShardIds().isEmpty()) {
            result.put(PARENT_SHARD_ID_KEY, DynamoUtils.createAttributeValue(lease.getParentShardIds()));
        }

        if (lease.getPendingCheckpoint() != null && !lease.getPendingCheckpoint().getSequenceNumber().isEmpty()) {
            result.put(PENDING_CHECKPOINT_SEQUENCE_KEY, DynamoUtils.createAttributeValue(lease.getPendingCheckpoint().getSequenceNumber()));
            result.put(PENDING_CHECKPOINT_SUBSEQUENCE_KEY, DynamoUtils.createAttributeValue(lease.getPendingCheckpoint().getSubSequenceNumber()));
        }

        return result;
    }

    @Override
    public KinesisClientLease fromDynamoRecord(Map<String, AttributeValue> dynamoRecord) {
        KinesisClientLease result = (KinesisClientLease) baseSerializer.fromDynamoRecord(dynamoRecord);

        result.setOwnerSwitchesSinceCheckpoint(DynamoUtils.safeGetLong(dynamoRecord, OWNER_SWITCHES_KEY));
        result.setCheckpoint(
            new ExtendedSequenceNumber(
                DynamoUtils.safeGetString(dynamoRecord, CHECKPOINT_SEQUENCE_NUMBER_KEY),
                DynamoUtils.safeGetLong(dynamoRecord, CHECKPOINT_SUBSEQUENCE_NUMBER_KEY))
        );
        result.setParentShardIds(DynamoUtils.safeGetSS(dynamoRecord, PARENT_SHARD_ID_KEY));

        if (!Strings.isNullOrEmpty(DynamoUtils.safeGetString(dynamoRecord, PENDING_CHECKPOINT_SEQUENCE_KEY))) {
            result.setPendingCheckpoint(
                new ExtendedSequenceNumber(
                    DynamoUtils.safeGetString(dynamoRecord, PENDING_CHECKPOINT_SEQUENCE_KEY),
                    DynamoUtils.safeGetLong(dynamoRecord, PENDING_CHECKPOINT_SUBSEQUENCE_KEY))
            );
        }

        return result;
    }

    @Override
    public Map<String, AttributeValue> getDynamoHashKey(KinesisClientLease lease) {
        return baseSerializer.getDynamoHashKey(lease);
    }

    @Override
    public Map<String, AttributeValue> getDynamoHashKey(String shardId) {
        return baseSerializer.getDynamoHashKey(shardId);
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoLeaseCounterExpectation(KinesisClientLease lease) {
        return baseSerializer.getDynamoLeaseCounterExpectation(lease);
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoLeaseOwnerExpectation(KinesisClientLease lease) {
        return baseSerializer.getDynamoLeaseOwnerExpectation(lease);
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoNonexistantExpectation() {
        return baseSerializer.getDynamoNonexistantExpectation();
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoLeaseCounterUpdate(KinesisClientLease lease) {
        return baseSerializer.getDynamoLeaseCounterUpdate(lease);
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoTakeLeaseUpdate(KinesisClientLease lease, String newOwner) {
        Map<String, AttributeValueUpdate> result = baseSerializer.getDynamoTakeLeaseUpdate(lease, newOwner);

        String oldOwner = lease.getLeaseOwner();
        if (oldOwner != null && !oldOwner.equals(newOwner)) {
            result.put(OWNER_SWITCHES_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(1L),
                    AttributeAction.ADD));
        }

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoEvictLeaseUpdate(KinesisClientLease lease) {
        return baseSerializer.getDynamoEvictLeaseUpdate(lease);
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(KinesisClientLease lease) {
        Map<String, AttributeValueUpdate> result = baseSerializer.getDynamoUpdateLeaseUpdate(lease);

        result.put(CHECKPOINT_SEQUENCE_NUMBER_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getCheckpoint().getSequenceNumber()),
                AttributeAction.PUT));
        result.put(CHECKPOINT_SUBSEQUENCE_NUMBER_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getCheckpoint().getSubSequenceNumber()),
                AttributeAction.PUT));
        result.put(OWNER_SWITCHES_KEY,
                new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getOwnerSwitchesSinceCheckpoint()),
                        AttributeAction.PUT));

        if (lease.getPendingCheckpoint() != null && !lease.getPendingCheckpoint().getSequenceNumber().isEmpty()) {
            result.put(PENDING_CHECKPOINT_SEQUENCE_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getPendingCheckpoint().getSequenceNumber()), AttributeAction.PUT));
            result.put(PENDING_CHECKPOINT_SUBSEQUENCE_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getPendingCheckpoint().getSubSequenceNumber()), AttributeAction.PUT));
        } else {
            result.put(PENDING_CHECKPOINT_SEQUENCE_KEY, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
            result.put(PENDING_CHECKPOINT_SUBSEQUENCE_KEY, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
        }

        return result;
    }

    @Override
    public Collection<KeySchemaElement> getKeySchema() {
        return baseSerializer.getKeySchema();
    }

    @Override
    public Collection<AttributeDefinition> getAttributeDefinitions() {
        return baseSerializer.getAttributeDefinitions();
    }

}
