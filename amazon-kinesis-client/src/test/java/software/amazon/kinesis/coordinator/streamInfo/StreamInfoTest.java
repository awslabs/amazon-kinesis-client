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
package software.amazon.kinesis.coordinator.streamInfo;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.EntityDAO;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.EntityType.CoordinatorStateType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link StreamInfo} entity type system.
 */
class StreamInfoTest {

    // --- Entity type tests ---

    @Test
    void streamInfo_implementsEntityInterface() {
        final StreamInfo streamInfo = new StreamInfo("stream-key-1", "stream-id-1");
        assertTrue(streamInfo instanceof EntityDAO.Entity, "StreamInfo should implement EntityDAO.Entity");
    }

    @Test
    void streamInfo_extendsCoordinatorState() {
        final StreamInfo streamInfo = new StreamInfo("stream-key-1", "stream-id-1");
        assertTrue(streamInfo instanceof CoordinatorState, "StreamInfo should extend CoordinatorState");
    }

    @Test
    void getEntityType_returnsStreamInfo() {
        final StreamInfo streamInfo = new StreamInfo("stream-key-1", "stream-id-1");
        assertNotNull(streamInfo.getEntityType());
        assertEquals(EntityType.STREAM_INFO, streamInfo.getEntityType());
    }

    @Test
    void getEntityType_ddbValue_isSTREAM() {
        final StreamInfo streamInfo = new StreamInfo("stream-key-1", "stream-id-1");
        assertEquals("STREAM", streamInfo.getEntityType().getDdbValue());
    }

    @Test
    void coordinatorStateEntityType_isStreamInfo() {
        final StreamInfo streamInfo = new StreamInfo("stream-key-1", "stream-id-1");
        assertEquals(CoordinatorStateType.STREAM_INFO, streamInfo.getCoordinatorStateEntityType());
    }

    // --- Serialization ---

    @Test
    void testSerialize() {
        final StreamInfo streamInfo = new StreamInfo("stream-key-1", "my-stream-id");
        final Map<String, AttributeValue> serialized = streamInfo.serialize();

        assertTrue(serialized.containsKey("entityType"), "Serialized StreamInfo should include entityType");
        assertEquals("STREAM", serialized.get("entityType").s());

        assertTrue(serialized.containsKey(StreamInfo.STREAM_ID_ATTRIBUTE_NAME));
        assertEquals(
                "my-stream-id",
                serialized.get(StreamInfo.STREAM_ID_ATTRIBUTE_NAME).s());
    }

    // --- Deserialization ---
    // Note: entityType attribute is stripped by the DAO delegate's resolveEntityType() before
    // calling the deserializer, so deserialize tests should NOT include entityType in attributes.

    @Test
    void deserialize_validAttributes_returnsStreamInfo() {
        final Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put(StreamInfo.STREAM_ID_ATTRIBUTE_NAME, AttributeValue.fromS("deserialized-stream-id"));

        final StreamInfo deserialized = StreamInfo.deserialize("stream-key-2", attributes);

        assertNotNull(deserialized);
        assertEquals("deserialized-stream-id", deserialized.getStreamId());
        assertEquals("stream-key-2", deserialized.getKey());
        // entityType is always set by the constructor, not from the attributes map
        assertEquals(EntityType.STREAM_INFO, deserialized.getEntityType());
    }

    @Test
    void deserialize_nullAttributes_returnsNull() {
        final StreamInfo deserialized = StreamInfo.deserialize("key", null);
        assertNull(deserialized);
    }

    @Test
    void deserialize_missingStreamId_returnsNull() {
        // Missing STREAM_ID_ATTRIBUTE_NAME causes an exception -> returns null
        final Map<String, AttributeValue> attributes = new HashMap<>();

        final StreamInfo deserialized = StreamInfo.deserialize("key", attributes);
        assertNull(deserialized);
    }

    @Test
    void serialize_thenDeserialize_roundTrip() {
        final StreamInfo original = new StreamInfo("key-1", "stream-456");
        final Map<String, AttributeValue> serialized = original.serialize();

        // Verify serialize includes entityType attribute (from super.serialize())
        assertTrue(serialized.containsKey("entityType"), "serialize() should include entityType");
        assertEquals("STREAM", serialized.get("entityType").s());

        // Simulate DAO behavior: remove entityType before passing to deserializer
        serialized.remove("entityType");

        final StreamInfo deserialized = StreamInfo.deserialize("key-1", serialized);
        assertNotNull(deserialized);
        assertEquals("stream-456", deserialized.getStreamId());
        assertEquals(EntityType.STREAM_INFO, deserialized.getEntityType());
        assertEquals(CoordinatorStateType.STREAM_INFO, deserialized.getCoordinatorStateEntityType());
    }

    // --- multiStreamLeaseKeyToStreamIdentifier utility ---

    @Test
    void multiStreamLeaseKeyToStreamIdentifier_validKey_extractsIdentifier() {
        final String result = StreamInfo.multiStreamLeaseKeyToStreamIdentifier("account:stream:123:shard-001");
        assertEquals("account:stream:123", result);
    }

    @Test
    void multiStreamLeaseKeyToStreamIdentifier_twoPartKey_returnsAsIs() {
        final String result = StreamInfo.multiStreamLeaseKeyToStreamIdentifier("simple:key");
        assertEquals("simple:key", result);
    }
}
