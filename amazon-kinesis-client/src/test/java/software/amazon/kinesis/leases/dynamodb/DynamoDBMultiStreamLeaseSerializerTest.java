/*
 * Copyright 2026 Amazon.com, Inc. or its affiliates.
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
import software.amazon.kinesis.leases.MultiStreamLease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class DynamoDBMultiStreamLeaseSerializerTest {

    private static final String STREAM_IDENTIFIER = "123456789012:test-stream:1";
    private static final String SHARD_ID = "shardId-000000000000";
    private static final String LEASE_KEY = STREAM_IDENTIFIER + ":" + SHARD_ID;

    private DynamoDBMultiStreamLeaseSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new DynamoDBMultiStreamLeaseSerializer();
    }

    @Test
    void fromDynamoRecord_withWorkerMetricStatsEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalMultiStreamDynamoRecord("worker-id-key");
        record.put("entityType", AttributeValue.fromS("WORKER_METRIC_STATS"));

        final MultiStreamLease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with WORKER_METRIC_STATS entityType should not be deserialized as a lease");
    }

    @Test
    void fromDynamoRecord_withLeaderLockEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalMultiStreamDynamoRecord("Leader");
        record.put("entityType", AttributeValue.fromS("LEADER"));

        final MultiStreamLease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with LEADER entityType should not be deserialized as a lease");
    }

    @Test
    void fromDynamoRecord_withClientVersionMigrationEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalMultiStreamDynamoRecord("Migration3.0");
        record.put("entityType", AttributeValue.fromS("CLIENT_VERSION_MIGRATION"));

        final MultiStreamLease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with CLIENT_VERSION_MIGRATION entityType should not be deserialized as a lease");
    }

    @Test
    void fromDynamoRecord_withUnknownEntityType_returnsNull() {
        final Map<String, AttributeValue> record = createMinimalMultiStreamDynamoRecord("unknown-key");
        record.put("entityType", AttributeValue.fromS("SOME_FUTURE_TYPE"));

        final MultiStreamLease result = serializer.fromDynamoRecord(record);

        assertNull(result, "Records with unknown entityType should not be deserialized as a lease");
    }

    @Test
    void fromDynamoRecord_withNoEntityType_deserializesAsMultiStreamLease() {
        final Map<String, AttributeValue> record = createMinimalMultiStreamDynamoRecord(LEASE_KEY);

        final MultiStreamLease result = serializer.fromDynamoRecord(record);

        assertNotNull(result, "Records without entityType should be deserialized as a lease");
        assertEquals(LEASE_KEY, result.leaseKey());
        assertEquals(STREAM_IDENTIFIER, result.streamIdentifier());
        assertEquals(SHARD_ID, result.shardId());
    }

    @Test
    void fromDynamoRecord_withLeaseEntityType_deserializesAsMultiStreamLease() {
        final Map<String, AttributeValue> record = createMinimalMultiStreamDynamoRecord(LEASE_KEY);
        record.put("entityType", AttributeValue.fromS("LEASE"));

        final MultiStreamLease result = serializer.fromDynamoRecord(record);

        assertNotNull(result, "Records with LEASE entityType should be deserialized");
        assertEquals(LEASE_KEY, result.leaseKey());
        assertEquals(STREAM_IDENTIFIER, result.streamIdentifier());
        assertEquals(SHARD_ID, result.shardId());
    }

    @Test
    void roundTrip_serializeDeserialize_preservesMultiStreamFields() {
        final MultiStreamLease original = createMinimalMultiStreamLease();
        original.leaseOwner("test-owner");
        original.leaseCounter(42L);

        final Map<String, AttributeValue> record = serializer.toDynamoRecord(original);
        final MultiStreamLease deserialized = serializer.fromDynamoRecord(record);

        assertNotNull(deserialized);
        assertEquals(LEASE_KEY, deserialized.leaseKey());
        assertEquals(STREAM_IDENTIFIER, deserialized.streamIdentifier());
        assertEquals(SHARD_ID, deserialized.shardId());
        assertEquals("test-owner", deserialized.leaseOwner());
        assertEquals(42L, deserialized.leaseCounter());
    }

    private MultiStreamLease createMinimalMultiStreamLease() {
        final MultiStreamLease lease = new MultiStreamLease();
        lease.leaseKey(LEASE_KEY);
        lease.streamIdentifier(STREAM_IDENTIFIER);
        lease.shardId(SHARD_ID);
        lease.leaseCounter(0L);
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.checkpoint(new software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber("TRIM_HORIZON"));
        return lease;
    }

    private Map<String, AttributeValue> createMinimalMultiStreamDynamoRecord(final String leaseKey) {
        final Map<String, AttributeValue> record = new HashMap<>();
        record.put("leaseKey", AttributeValue.fromS(leaseKey));
        record.put("leaseCounter", AttributeValue.fromN("0"));
        record.put("ownerSwitchesSinceCheckpoint", AttributeValue.fromN("0"));
        record.put("checkpoint", AttributeValue.fromS("TRIM_HORIZON"));
        record.put("checkpointSubSequenceNumber", AttributeValue.fromN("0"));
        record.put("streamName", AttributeValue.fromS(STREAM_IDENTIFIER));
        record.put("shardId", AttributeValue.fromS(SHARD_ID));
        return record;
    }
}
