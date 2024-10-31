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
package software.amazon.kinesis.leases.dynamodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.HashKeyRangeForLease;
import software.amazon.kinesis.leases.DynamoUtils;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.UpdateField;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * An implementation of ILeaseSerializer for basic Lease objects. Can also instantiate subclasses of Lease so that
 * LeaseSerializer can be decorated by other classes if you need to add fields to leases.
 */
@KinesisClientInternalApi
public class DynamoDBLeaseSerializer implements LeaseSerializer {
    private static final String LEASE_COUNTER_KEY = "leaseCounter";
    private static final String OWNER_SWITCHES_KEY = "ownerSwitchesSinceCheckpoint";
    private static final String CHECKPOINT_SUBSEQUENCE_NUMBER_KEY = "checkpointSubSequenceNumber";
    private static final String PENDING_CHECKPOINT_SEQUENCE_KEY = "pendingCheckpoint";
    private static final String PENDING_CHECKPOINT_SUBSEQUENCE_KEY = "pendingCheckpointSubSequenceNumber";
    private static final String PENDING_CHECKPOINT_STATE_KEY = "pendingCheckpointState";
    private static final String PARENT_SHARD_ID_KEY = "parentShardId";
    private static final String CHILD_SHARD_IDS_KEY = "childShardIds";
    private static final String STARTING_HASH_KEY = "startingHashKey";
    private static final String ENDING_HASH_KEY = "endingHashKey";
    private static final String THROUGHOUT_PUT_KBPS = "throughputKBps";
    private static final String CHECKPOINT_SEQUENCE_NUMBER_KEY = "checkpoint";
    static final String CHECKPOINT_OWNER = "checkpointOwner";
    static final String LEASE_OWNER_KEY = "leaseOwner";
    static final String LEASE_KEY_KEY = "leaseKey";

    @Override
    public Map<String, AttributeValue> toDynamoRecord(final Lease lease) {
        Map<String, AttributeValue> result = new HashMap<>();

        result.put(LEASE_KEY_KEY, DynamoUtils.createAttributeValue(lease.leaseKey()));
        result.put(LEASE_COUNTER_KEY, DynamoUtils.createAttributeValue(lease.leaseCounter()));

        if (lease.leaseOwner() != null) {
            result.put(LEASE_OWNER_KEY, DynamoUtils.createAttributeValue(lease.leaseOwner()));
        }

        result.put(OWNER_SWITCHES_KEY, DynamoUtils.createAttributeValue(lease.ownerSwitchesSinceCheckpoint()));
        result.put(
                CHECKPOINT_SEQUENCE_NUMBER_KEY,
                DynamoUtils.createAttributeValue(lease.checkpoint().sequenceNumber()));
        result.put(
                CHECKPOINT_SUBSEQUENCE_NUMBER_KEY,
                DynamoUtils.createAttributeValue(lease.checkpoint().subSequenceNumber()));
        if (lease.parentShardIds() != null && !lease.parentShardIds().isEmpty()) {
            result.put(PARENT_SHARD_ID_KEY, DynamoUtils.createAttributeValue(lease.parentShardIds()));
        }
        if (!CollectionUtils.isNullOrEmpty(lease.childShardIds())) {
            result.put(CHILD_SHARD_IDS_KEY, DynamoUtils.createAttributeValue(lease.childShardIds()));
        }

        if (lease.pendingCheckpoint() != null
                && !lease.pendingCheckpoint().sequenceNumber().isEmpty()) {
            result.put(
                    PENDING_CHECKPOINT_SEQUENCE_KEY,
                    DynamoUtils.createAttributeValue(lease.pendingCheckpoint().sequenceNumber()));
            result.put(
                    PENDING_CHECKPOINT_SUBSEQUENCE_KEY,
                    DynamoUtils.createAttributeValue(lease.pendingCheckpoint().subSequenceNumber()));
        }

        if (lease.pendingCheckpointState() != null) {
            result.put(
                    PENDING_CHECKPOINT_STATE_KEY,
                    DynamoUtils.createAttributeValue(lease.checkpoint().subSequenceNumber()));
        }

        if (lease.hashKeyRangeForLease() != null) {
            result.put(
                    STARTING_HASH_KEY,
                    DynamoUtils.createAttributeValue(
                            lease.hashKeyRangeForLease().serializedStartingHashKey()));
            result.put(
                    ENDING_HASH_KEY,
                    DynamoUtils.createAttributeValue(
                            lease.hashKeyRangeForLease().serializedEndingHashKey()));
        }

        if (lease.throughputKBps() != null) {
            result.put(THROUGHOUT_PUT_KBPS, DynamoUtils.createAttributeValue(lease.throughputKBps()));
        }

        if (lease.checkpointOwner() != null) {
            result.put(CHECKPOINT_OWNER, DynamoUtils.createAttributeValue(lease.checkpointOwner()));
        }
        return result;
    }

    @Override
    public Lease fromDynamoRecord(final Map<String, AttributeValue> dynamoRecord) {
        final Lease result = new Lease();
        return fromDynamoRecord(dynamoRecord, result);
    }

    @Override
    public Lease fromDynamoRecord(Map<String, AttributeValue> dynamoRecord, Lease leaseToUpdate) {
        leaseToUpdate.leaseKey(DynamoUtils.safeGetString(dynamoRecord, LEASE_KEY_KEY));
        leaseToUpdate.leaseOwner(DynamoUtils.safeGetString(dynamoRecord, LEASE_OWNER_KEY));
        leaseToUpdate.leaseCounter(DynamoUtils.safeGetLong(dynamoRecord, LEASE_COUNTER_KEY));

        leaseToUpdate.ownerSwitchesSinceCheckpoint(DynamoUtils.safeGetLong(dynamoRecord, OWNER_SWITCHES_KEY));
        leaseToUpdate.checkpoint(new ExtendedSequenceNumber(
                DynamoUtils.safeGetString(dynamoRecord, CHECKPOINT_SEQUENCE_NUMBER_KEY),
                DynamoUtils.safeGetLong(dynamoRecord, CHECKPOINT_SUBSEQUENCE_NUMBER_KEY)));
        leaseToUpdate.parentShardIds(DynamoUtils.safeGetSS(dynamoRecord, PARENT_SHARD_ID_KEY));
        leaseToUpdate.childShardIds(DynamoUtils.safeGetSS(dynamoRecord, CHILD_SHARD_IDS_KEY));

        if (!Strings.isNullOrEmpty(DynamoUtils.safeGetString(dynamoRecord, PENDING_CHECKPOINT_SEQUENCE_KEY))) {
            leaseToUpdate.pendingCheckpoint(new ExtendedSequenceNumber(
                    DynamoUtils.safeGetString(dynamoRecord, PENDING_CHECKPOINT_SEQUENCE_KEY),
                    DynamoUtils.safeGetLong(dynamoRecord, PENDING_CHECKPOINT_SUBSEQUENCE_KEY)));
        }

        leaseToUpdate.pendingCheckpointState(DynamoUtils.safeGetByteArray(dynamoRecord, PENDING_CHECKPOINT_STATE_KEY));

        final String startingHashKey, endingHashKey;
        if (!Strings.isNullOrEmpty(startingHashKey = DynamoUtils.safeGetString(dynamoRecord, STARTING_HASH_KEY))
                && !Strings.isNullOrEmpty(endingHashKey = DynamoUtils.safeGetString(dynamoRecord, ENDING_HASH_KEY))) {
            leaseToUpdate.hashKeyRange(HashKeyRangeForLease.deserialize(startingHashKey, endingHashKey));
        }

        if (DynamoUtils.safeGetDouble(dynamoRecord, THROUGHOUT_PUT_KBPS) != null) {
            leaseToUpdate.throughputKBps(DynamoUtils.safeGetDouble(dynamoRecord, THROUGHOUT_PUT_KBPS));
        }

        if (DynamoUtils.safeGetString(dynamoRecord, CHECKPOINT_OWNER) != null) {
            leaseToUpdate.checkpointOwner(DynamoUtils.safeGetString(dynamoRecord, CHECKPOINT_OWNER));
        }

        return leaseToUpdate;
    }

    @Override
    public Map<String, AttributeValue> getDynamoHashKey(final String leaseKey) {
        Map<String, AttributeValue> result = new HashMap<>();

        result.put(LEASE_KEY_KEY, DynamoUtils.createAttributeValue(leaseKey));

        return result;
    }

    @Override
    public Map<String, AttributeValue> getDynamoHashKey(final Lease lease) {
        return getDynamoHashKey(lease.leaseKey());
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoLeaseCounterExpectation(final Lease lease) {
        return getDynamoLeaseCounterExpectation(lease.leaseCounter());
    }

    public Map<String, ExpectedAttributeValue> getDynamoLeaseCounterExpectation(final Long leaseCounter) {
        Map<String, ExpectedAttributeValue> result = new HashMap<>();

        ExpectedAttributeValue eav = ExpectedAttributeValue.builder()
                .value(DynamoUtils.createAttributeValue(leaseCounter))
                .build();
        result.put(LEASE_COUNTER_KEY, eav);

        return result;
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoLeaseOwnerExpectation(final Lease lease) {
        final Map<String, ExpectedAttributeValue> result = new HashMap<>();
        result.put(LEASE_OWNER_KEY, buildExpectedAttributeValueIfExistsOrValue(lease.leaseOwner()));
        result.put(CHECKPOINT_OWNER, buildExpectedAttributeValueIfExistsOrValue(lease.checkpointOwner()));
        return result;
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoNonexistantExpectation() {
        Map<String, ExpectedAttributeValue> result = new HashMap<>();

        ExpectedAttributeValue expectedAV =
                ExpectedAttributeValue.builder().exists(false).build();
        result.put(LEASE_KEY_KEY, expectedAV);

        return result;
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoExistentExpectation(String leaseKey) {
        Map<String, ExpectedAttributeValue> result = new HashMap<>();

        ExpectedAttributeValue expectedAV = ExpectedAttributeValue.builder()
                .exists(true)
                .value(DynamoUtils.createAttributeValue(leaseKey))
                .build();
        result.put(LEASE_KEY_KEY, expectedAV);

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoLeaseCounterUpdate(final Lease lease) {
        return getDynamoLeaseCounterUpdate(lease.leaseCounter());
    }

    public Map<String, AttributeValueUpdate> getDynamoLeaseCounterUpdate(Long leaseCounter) {
        Map<String, AttributeValueUpdate> result = new HashMap<>();

        AttributeValueUpdate avu = AttributeValueUpdate.builder()
                .value(DynamoUtils.createAttributeValue(leaseCounter + 1))
                .action(AttributeAction.PUT)
                .build();
        result.put(LEASE_COUNTER_KEY, avu);

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoTakeLeaseUpdate(final Lease lease, String owner) {
        Map<String, AttributeValueUpdate> result = new HashMap<>();

        result.put(
                LEASE_OWNER_KEY,
                AttributeValueUpdate.builder()
                        .value(DynamoUtils.createAttributeValue(owner))
                        .action(AttributeAction.PUT)
                        .build());
        // this method is currently used by assignLease and takeLease. In both case we want the checkpoint owner to be
        // deleted as this is a fresh assignment
        result.put(
                CHECKPOINT_OWNER,
                AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());

        String oldOwner = lease.leaseOwner();
        String checkpointOwner = lease.checkpointOwner();
        // if checkpoint owner is not null, this update is supposed to remove the checkpoint owner
        // and transfer the lease ownership to the leaseOwner so incrementing the owner switch key
        if (oldOwner != null && !oldOwner.equals(owner) || (checkpointOwner != null && checkpointOwner.equals(owner))) {
            result.put(
                    OWNER_SWITCHES_KEY,
                    AttributeValueUpdate.builder()
                            .value(DynamoUtils.createAttributeValue(1L))
                            .action(AttributeAction.ADD)
                            .build());
        }

        return result;
    }

    /**
     * AssignLease performs the PUT action on the LeaseOwner and ADD (1) action on the leaseCounter.
     * @param lease lease that needs to be assigned
     * @param newOwner newLeaseOwner
     * @return Map of AttributeName to update operation
     */
    @Override
    public Map<String, AttributeValueUpdate> getDynamoAssignLeaseUpdate(final Lease lease, final String newOwner) {
        Map<String, AttributeValueUpdate> result = getDynamoTakeLeaseUpdate(lease, newOwner);

        result.put(LEASE_COUNTER_KEY, getAttributeValueUpdateForAdd());
        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoEvictLeaseUpdate(final Lease lease) {
        final Map<String, AttributeValueUpdate> result = new HashMap<>();
        // if checkpointOwner is not null, it means lease handoff is initiated. In this case we just remove the
        // checkpoint owner so the next owner (leaseOwner) can pick up the lease without waiting for assignment.
        // Otherwise, remove the leaseOwner
        if (lease.checkpointOwner() == null) {
            result.put(
                    LEASE_OWNER_KEY,
                    AttributeValueUpdate.builder()
                            .action(AttributeAction.DELETE)
                            .build());
        }
        // We always want to remove checkpointOwner, it's ok even if it's null
        result.put(
                CHECKPOINT_OWNER,
                AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
        result.put(LEASE_COUNTER_KEY, getAttributeValueUpdateForAdd());
        return result;
    }

    protected AttributeValueUpdate putUpdate(AttributeValue attributeValue) {
        return AttributeValueUpdate.builder()
                .value(attributeValue)
                .action(AttributeAction.PUT)
                .build();
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(final Lease lease) {
        Map<String, AttributeValueUpdate> result = new HashMap<>();
        result.put(
                CHECKPOINT_SEQUENCE_NUMBER_KEY,
                putUpdate(DynamoUtils.createAttributeValue(lease.checkpoint().sequenceNumber())));
        result.put(
                CHECKPOINT_SUBSEQUENCE_NUMBER_KEY,
                putUpdate(DynamoUtils.createAttributeValue(lease.checkpoint().subSequenceNumber())));
        result.put(
                OWNER_SWITCHES_KEY, putUpdate(DynamoUtils.createAttributeValue(lease.ownerSwitchesSinceCheckpoint())));

        if (lease.pendingCheckpoint() != null
                && !lease.pendingCheckpoint().sequenceNumber().isEmpty()) {
            result.put(
                    PENDING_CHECKPOINT_SEQUENCE_KEY,
                    putUpdate(DynamoUtils.createAttributeValue(
                            lease.pendingCheckpoint().sequenceNumber())));
            result.put(
                    PENDING_CHECKPOINT_SUBSEQUENCE_KEY,
                    putUpdate(DynamoUtils.createAttributeValue(
                            lease.pendingCheckpoint().subSequenceNumber())));
        } else {
            result.put(
                    PENDING_CHECKPOINT_SEQUENCE_KEY,
                    AttributeValueUpdate.builder()
                            .action(AttributeAction.DELETE)
                            .build());
            result.put(
                    PENDING_CHECKPOINT_SUBSEQUENCE_KEY,
                    AttributeValueUpdate.builder()
                            .action(AttributeAction.DELETE)
                            .build());
        }

        if (lease.pendingCheckpointState() != null) {
            result.put(
                    PENDING_CHECKPOINT_STATE_KEY,
                    putUpdate(DynamoUtils.createAttributeValue(lease.pendingCheckpointState())));
        } else {
            result.put(
                    PENDING_CHECKPOINT_STATE_KEY,
                    AttributeValueUpdate.builder()
                            .action(AttributeAction.DELETE)
                            .build());
        }

        if (!CollectionUtils.isNullOrEmpty(lease.childShardIds())) {
            result.put(CHILD_SHARD_IDS_KEY, putUpdate(DynamoUtils.createAttributeValue(lease.childShardIds())));
        }

        if (lease.hashKeyRangeForLease() != null) {
            result.put(
                    STARTING_HASH_KEY,
                    putUpdate(DynamoUtils.createAttributeValue(
                            lease.hashKeyRangeForLease().serializedStartingHashKey())));
            result.put(
                    ENDING_HASH_KEY,
                    putUpdate(DynamoUtils.createAttributeValue(
                            lease.hashKeyRangeForLease().serializedEndingHashKey())));
        }

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(Lease lease, UpdateField updateField) {
        Map<String, AttributeValueUpdate> result = new HashMap<>();
        switch (updateField) {
            case CHILD_SHARDS:
                if (!CollectionUtils.isNullOrEmpty(lease.childShardIds())) {
                    result.put(CHILD_SHARD_IDS_KEY, putUpdate(DynamoUtils.createAttributeValue(lease.childShardIds())));
                }
                break;
            case HASH_KEY_RANGE:
                if (lease.hashKeyRangeForLease() != null) {
                    result.put(
                            STARTING_HASH_KEY,
                            putUpdate(DynamoUtils.createAttributeValue(
                                    lease.hashKeyRangeForLease().serializedStartingHashKey())));
                    result.put(
                            ENDING_HASH_KEY,
                            putUpdate(DynamoUtils.createAttributeValue(
                                    lease.hashKeyRangeForLease().serializedEndingHashKey())));
                }
                break;
        }
        return result;
    }

    @Override
    public Collection<KeySchemaElement> getKeySchema() {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder()
                .attributeName(LEASE_KEY_KEY)
                .keyType(KeyType.HASH)
                .build());

        return keySchema;
    }

    @Override
    public Collection<AttributeDefinition> getAttributeDefinitions() {
        List<AttributeDefinition> definitions = new ArrayList<>();
        definitions.add(AttributeDefinition.builder()
                .attributeName(LEASE_KEY_KEY)
                .attributeType(ScalarAttributeType.S)
                .build());

        return definitions;
    }

    @Override
    public Collection<KeySchemaElement> getWorkerIdToLeaseKeyIndexKeySchema() {
        final List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder()
                .attributeName(LEASE_OWNER_KEY)
                .keyType(KeyType.HASH)
                .build());
        keySchema.add(KeySchemaElement.builder()
                .attributeName(LEASE_KEY_KEY)
                .keyType(KeyType.RANGE)
                .build());
        return keySchema;
    }

    @Override
    public Collection<AttributeDefinition> getWorkerIdToLeaseKeyIndexAttributeDefinitions() {
        final List<AttributeDefinition> definitions = new ArrayList<>();
        definitions.add(AttributeDefinition.builder()
                .attributeName(LEASE_OWNER_KEY)
                .attributeType(ScalarAttributeType.S)
                .build());
        definitions.add(AttributeDefinition.builder()
                .attributeName(LEASE_KEY_KEY)
                .attributeType(ScalarAttributeType.S)
                .build());
        return definitions;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoLeaseThroughputKbpsUpdate(Lease lease) {
        final Map<String, AttributeValueUpdate> result = new HashMap<>();
        final AttributeValueUpdate avu = AttributeValueUpdate.builder()
                .value(DynamoUtils.createAttributeValue(lease.throughputKBps()))
                .action(AttributeAction.PUT)
                .build();
        result.put(THROUGHOUT_PUT_KBPS, avu);
        return result;
    }

    private static ExpectedAttributeValue buildExpectedAttributeValueIfExistsOrValue(String value) {
        return value == null
                ? ExpectedAttributeValue.builder().exists(false).build()
                : ExpectedAttributeValue.builder()
                        .value(DynamoUtils.createAttributeValue(value))
                        .build();
    }

    private static AttributeValueUpdate getAttributeValueUpdateForAdd() {
        return AttributeValueUpdate.builder()
                .value(DynamoUtils.createAttributeValue(1L))
                .action(AttributeAction.ADD)
                .build();
    }
}
