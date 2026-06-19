/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.Lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for entity type handling in DynamoDBLeaseSerializer:
 * - toDynamoRecord includes entityType attribute
 * - fromDynamoRecord handles null entityType (backward compat)
 * - fromDynamoRecord handles LEASE entityType
 * - fromDynamoRecord returns null for non-LEASE entity types
 */
class DynamoDBLeaseSerializerTest {

    private DynamoDBLeaseSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new DynamoDBLeaseSerializer();
    }

    // --- toDynamoRecord tests ---

    @Test
    void toDynamoRecord_includesEntityTypeAttribute() {
        final Lease lease = createMinimalLease("shard-001");

        final Map<String, AttributeValue> record = serializer.toDynamoRecord(lease);

        assertTrue(record.containsKey("entityType"), "Serialized lease should contain entityType attribute");
        assertEquals("LEASE", record.get("entityType").s());
    }

    @Test
    void toDynamoRecord_entityTypeIsLease() {
        final Lease lease = createMinimalLease("shard-002");
        lease.leaseOwner("worker-1");

        final Map<String, AttributeValue> record = serializer.toDynamoRecord(lease);

        assertEquals(EntityType.LEASE.getDdbValue(), record.get("entityType").s());
    }

    // --- fromDynamoRecord backward compatibility tests ---

    @Test
    void fromDynamoRecord_withNoEntityType_deserializesAsLease() {
        // Backward compatibility: items without entityType are treated as leases
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("shard-001");
        // Deliberately do NOT put "entityType" attribute

        final Lease result = serializer.fromDynamoRecord(record);

        assertNotNull(result, "Records without entityType should be deserialized as Lease");
        assertEquals("shard-001", result.leaseKey());
    }

    @Test
    void fromDynamoRecord_withLeaseEntityType_deserializesAsLease() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("shard-002");
        record.put("entityType", AttributeValue.fromS("LEASE"));

        final Lease result = serializer.fromDynamoRecord(record);

        assertNotNull(result, "Records with LEASE entityType should be deserialized");
        assertEquals("shard-002", result.leaseKey());
    }

    @Test
    void fromDynamoRecord_withWorkerMetricStatsEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("worker-metrics-key");
        record.put("entityType", AttributeValue.fromS("WORKER_METRIC_STATS"));

        final Lease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with WORKER_METRIC_STATS entityType should not be deserialized as Lease");
    }

    @Test
    void fromDynamoRecord_withStreamInfoEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("stream-info-key");
        record.put("entityType", AttributeValue.fromS("STREAM"));

        final Lease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with STREAM entityType should not be deserialized as Lease");
    }

    @Test
    void fromDynamoRecord_withLeaderLockEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("leader-key");
        record.put("entityType", AttributeValue.fromS("LEADER"));

        final Lease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with LEADER entityType should not be deserialized as Lease");
    }

    @Test
    void fromDynamoRecord_withClientVersionMigrationEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("migration-key");
        record.put("entityType", AttributeValue.fromS("CLIENT_VERSION_MIGRATION"));

        final Lease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with CLIENT_VERSION_MIGRATION entityType should not be deserialized as Lease");
    }

    @Test
    void fromDynamoRecord_withTableMigrationEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("table-mig-key");
        record.put("entityType", AttributeValue.fromS("TABLE_MIGRATION"));

        final Lease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with TABLE_MIGRATION entityType should not be deserialized as Lease");
    }

    @Test
    void fromDynamoRecord_withUnknownEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("unknown-key");
        record.put("entityType", AttributeValue.fromS("SOME_FUTURE_TYPE"));

        final Lease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with unknown entityType should not be deserialized as Lease");
    }

    // --- fromDynamoRecord with existing lease (update) ---

    @Test
    void fromDynamoRecord_withExistingLease_noEntityType_updatesLease() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("shard-003");
        final Lease existingLease = new Lease();

        final Lease result = serializer.fromDynamoRecord(record, existingLease);

        assertNotNull(result);
        assertEquals("shard-003", result.leaseKey());
    }

    @Test
    void fromDynamoRecord_withExistingLease_leaseEntityType_updatesLease() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("shard-004");
        record.put("entityType", AttributeValue.fromS("LEASE"));
        final Lease existingLease = new Lease();

        final Lease result = serializer.fromDynamoRecord(record, existingLease);

        assertNotNull(result);
        assertEquals("shard-004", result.leaseKey());
    }

    @Test
    void fromDynamoRecord_withExistingLease_nonLeaseEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalDynamoRecord("worker-key");
        record.put("entityType", AttributeValue.fromS("WORKER_METRIC_STATS"));
        final Lease existingLease = new Lease();

        final Lease result = serializer.fromDynamoRecord(record, existingLease);

        assertNull(result);
    }

    // --- Round-trip test ---

    @Test
    void roundTrip_serializeDeserialize_preservesLeaseFields() {
        final Lease original = createMinimalLease("shard-round-trip");
        original.leaseOwner("test-owner");
        original.leaseCounter(42L);

        final Map<String, AttributeValue> record = serializer.toDynamoRecord(original);
        final Lease deserialized = serializer.fromDynamoRecord(record);

        assertNotNull(deserialized);
        assertEquals("shard-round-trip", deserialized.leaseKey());
        assertEquals("test-owner", deserialized.leaseOwner());
        assertEquals(42L, deserialized.leaseCounter());
    }

    @Test
    void entityTypeConstant_matchesLeaseSerializerConstant() {
        assertEquals("entityType", DynamoDBLeaseSerializer.ENTITY_TYPE_ATTRIBUTE_NAME);
    }

    // --- Helper methods ---

    private Lease createMinimalLease(final String leaseKey) {
        final Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.leaseCounter(0L);
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.checkpoint(new software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber("TRIM_HORIZON"));
        return lease;
    }

    private Map<String, AttributeValue> createMinimalDynamoRecord(final String leaseKey) {
        final Map<String, AttributeValue> record = new HashMap<>();
        record.put("leaseKey", AttributeValue.fromS(leaseKey));
        record.put("leaseCounter", AttributeValue.fromN("0"));
        record.put("ownerSwitchesSinceCheckpoint", AttributeValue.fromN("0"));
        record.put("checkpoint", AttributeValue.fromS("TRIM_HORIZON"));
        record.put("checkpointSubSequenceNumber", AttributeValue.fromN("0"));
        return record;
    }
}
