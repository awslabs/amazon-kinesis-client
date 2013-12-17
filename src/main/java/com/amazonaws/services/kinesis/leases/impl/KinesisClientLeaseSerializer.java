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
package com.amazonaws.services.kinesis.leases.impl;

import java.util.Collection;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseSerializer;
import com.amazonaws.services.kinesis.leases.util.DynamoUtils;

/**
 * An implementation of ILeaseSerializer for KinesisClientLease objects.
 */
public class KinesisClientLeaseSerializer implements ILeaseSerializer<KinesisClientLease> {

    private static final String OWNER_SWITCHES_KEY = "ownerSwitchesSinceCheckpoint";
    private static final String CHECKPOINT_KEY = "checkpoint";
    public final String PARENT_SHARD_ID_KEY = "parentShardId";

    private final LeaseSerializer baseSerializer = new LeaseSerializer(KinesisClientLease.class);

    @Override
    public Map<String, AttributeValue> toDynamoRecord(KinesisClientLease lease) {
        Map<String, AttributeValue> result = baseSerializer.toDynamoRecord(lease);

        result.put(OWNER_SWITCHES_KEY, DynamoUtils.createAttributeValue(lease.getOwnerSwitchesSinceCheckpoint()));
        result.put(CHECKPOINT_KEY, DynamoUtils.createAttributeValue(lease.getCheckpoint()));
        if (lease.getParentShardIds() != null && !lease.getParentShardIds().isEmpty()) {
            result.put(PARENT_SHARD_ID_KEY, DynamoUtils.createAttributeValue(lease.getParentShardIds()));
        }

        return result;
    }

    @Override
    public KinesisClientLease fromDynamoRecord(Map<String, AttributeValue> dynamoRecord) {
        KinesisClientLease result = (KinesisClientLease) baseSerializer.fromDynamoRecord(dynamoRecord);

        result.setOwnerSwitchesSinceCheckpoint(DynamoUtils.safeGetLong(dynamoRecord, OWNER_SWITCHES_KEY));
        result.setCheckpoint(DynamoUtils.safeGetString(dynamoRecord, CHECKPOINT_KEY));
        result.setParentShardIds(DynamoUtils.safeGetSS(dynamoRecord, PARENT_SHARD_ID_KEY));

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

        Long ownerSwitchesSinceCheckpoint = lease.getOwnerSwitchesSinceCheckpoint();
        String oldOwner = lease.getLeaseOwner();
        if (oldOwner != null && !oldOwner.equals(newOwner)) {
            ownerSwitchesSinceCheckpoint++;
        }

        result.put(OWNER_SWITCHES_KEY,
                new AttributeValueUpdate(DynamoUtils.createAttributeValue(ownerSwitchesSinceCheckpoint),
                        AttributeAction.PUT));

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoEvictLeaseUpdate(KinesisClientLease lease) {
        return baseSerializer.getDynamoEvictLeaseUpdate(lease);
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(KinesisClientLease lease) {
        Map<String, AttributeValueUpdate> result = baseSerializer.getDynamoUpdateLeaseUpdate(lease);

        result.put(CHECKPOINT_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getCheckpoint()),
                AttributeAction.PUT));
        result.put(OWNER_SWITCHES_KEY,
                new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.getOwnerSwitchesSinceCheckpoint()),
                        AttributeAction.PUT));

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
