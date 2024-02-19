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
package com.amazonaws.services.kinesis.leases.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseSerializer;
import com.amazonaws.services.kinesis.leases.util.DynamoUtils;
import com.amazonaws.util.CollectionUtils;
import com.google.common.base.Strings;

import static com.amazonaws.services.kinesis.leases.impl.UpdateField.HASH_KEY_RANGE;

/**
 * An implementation of ILeaseSerializer for KinesisClientLease objects.
 */
public class KinesisClientLeaseSerializer implements ILeaseSerializer<KinesisClientLease> {

    private static final String OWNER_SWITCHES_KEY = "ownerSwitchesSinceCheckpoint";
    private static final String CHECKPOINT_SEQUENCE_NUMBER_KEY = "checkpoint";
    private static final String CHECKPOINT_SUBSEQUENCE_NUMBER_KEY = "checkpointSubSequenceNumber";
    private static final String PENDING_CHECKPOINT_SEQUENCE_KEY = "pendingCheckpoint";
    private static final String PENDING_CHECKPOINT_SUBSEQUENCE_KEY = "pendingCheckpointSubSequenceNumber";
    public final String PARENT_SHARD_ID_KEY = "parentShardId";
    public final String CHILD_SHARD_IDS_KEY = "childShardIds";
    private static final String STARTING_HASH_KEY = "startingHashKey";
    private static final String ENDING_HASH_KEY = "endingHashKey";

    private final LeaseSerializer baseSerializer = new LeaseSerializer(KinesisClientLease.class);

    @Override
    public Map<String, AttributeValue> toDynamoRecord(KinesisClientLease lease) {
        Map<String, AttributeValue> result = baseSerializer.toDynamoRecord(lease);

        result.put(OWNER_SWITCHES_KEY, DynamoUtils.createAttributeValue(lease.getOwnerSwitchesSinceCheckpoint()));
        result.put(CHECKPOINT_SEQUENCE_NUMBER_KEY, DynamoUtils.createAttributeValue(lease.getCheckpoint().getSequenceNumber()));
        result.put(CHECKPOINT_SUBSEQUENCE_NUMBER_KEY, DynamoUtils.createAttributeValue(lease.getCheckpoint().getSubSequenceNumber()));
        if (!CollectionUtils.isNullOrEmpty(lease.getParentShardIds())) {
            result.put(PARENT_SHARD_ID_KEY, DynamoUtils.createAttributeValue(lease.getParentShardIds()));
        }
        if (!CollectionUtils.isNullOrEmpty(lease.getChildShardIds())) {
            result.put(CHILD_SHARD_IDS_KEY, DynamoUtils.createAttributeValue(lease.getChildShardIds()));
        }

        if (lease.getPendingCheckpoint() != null && !lease.getPendingCheckpoint().getSequenceNumber().isEmpty()) {
            result.put(PENDING_CHECKPOINT_SEQUENCE_KEY, DynamoUtils.createAttributeValue(lease.getPendingCheckpoint().getSequenceNumber()));
            result.put(PENDING_CHECKPOINT_SUBSEQUENCE_KEY, DynamoUtils.createAttributeValue(lease.getPendingCheckpoint().getSubSequenceNumber()));
        }

        if(lease.getHashKeyRange() != null) {
            result.put(STARTING_HASH_KEY, DynamoUtils.createAttributeValue(lease.getHashKeyRange().serializedStartingHashKey()));
            result.put(ENDING_HASH_KEY, DynamoUtils.createAttributeValue(lease.getHashKeyRange().serializedEndingHashKey()));
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
        result.setChildShardIds(DynamoUtils.safeGetSS(dynamoRecord, CHILD_SHARD_IDS_KEY));

        if (!Strings.isNullOrEmpty(DynamoUtils.safeGetString(dynamoRecord, PENDING_CHECKPOINT_SEQUENCE_KEY))) {
            result.setPendingCheckpoint(
                new ExtendedSequenceNumber(
                    DynamoUtils.safeGetString(dynamoRecord, PENDING_CHECKPOINT_SEQUENCE_KEY),
                    DynamoUtils.safeGetLong(dynamoRecord, PENDING_CHECKPOINT_SUBSEQUENCE_KEY))
            );
        }

        final String startingHashKey, endingHashKey;
        if (!Strings.isNullOrEmpty(startingHashKey = DynamoUtils.safeGetString(dynamoRecord, STARTING_HASH_KEY))
                && !Strings.isNullOrEmpty(endingHashKey = DynamoUtils.safeGetString(dynamoRecord, ENDING_HASH_KEY))) {
            result.setHashKeyRange(HashKeyRangeForLease.deserialize(startingHashKey, endingHashKey));
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
    public Map<String, ExpectedAttributeValue> getDynamoLeaseCheckpointExpectation(KinesisClientLease lease) {
        Map<String, ExpectedAttributeValue> result = baseSerializer.getDynamoLeaseCheckpointExpectation(lease);
        ExpectedAttributeValue eav;

        if (!lease.getCheckpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
            eav = new ExpectedAttributeValue(DynamoUtils.createAttributeValue(ExtendedSequenceNumber.SHARD_END.getSequenceNumber()));
            eav.setComparisonOperator(ComparisonOperator.NE);
            result.put(CHECKPOINT_SEQUENCE_NUMBER_KEY, eav);
        }
        return result;
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoNonexistantExpectation() {
        return baseSerializer.getDynamoNonexistantExpectation();
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoExistentExpectation(final String leaseKey) {
        return baseSerializer.getDynamoExistentExpectation(leaseKey);
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
        if (!CollectionUtils.isNullOrEmpty(lease.getChildShardIds())) {
            result.put(CHILD_SHARD_IDS_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getChildShardIds()), AttributeAction.PUT));
        }

        if(lease.getHashKeyRange() != null) {
            result.put(STARTING_HASH_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getHashKeyRange().serializedStartingHashKey()), AttributeAction.PUT));
            result.put(ENDING_HASH_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getHashKeyRange().serializedEndingHashKey()), AttributeAction.PUT));
        }

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
    public Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(KinesisClientLease lease,
                                                                        UpdateField updateField) {
        Map<String, AttributeValueUpdate> result = new HashMap<>();

        switch (updateField) {
            case CHILD_SHARDS:
                if (!CollectionUtils.isNullOrEmpty(lease.getChildShardIds())) {
                    result.put(CHILD_SHARD_IDS_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(
                            lease.getChildShardIds()), AttributeAction.PUT));
                }
                break;
            case HASH_KEY_RANGE:
                if (lease.getHashKeyRange() != null) {
                    result.put(STARTING_HASH_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(
                            lease.getHashKeyRange().serializedStartingHashKey()), AttributeAction.PUT));
                    result.put(ENDING_HASH_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(
                            lease.getHashKeyRange().serializedEndingHashKey()), AttributeAction.PUT));
                }
                break;
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
