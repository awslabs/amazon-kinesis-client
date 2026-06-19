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
package software.amazon.kinesis.coordinator.migration;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.EntityDAO;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.EntityType.CoordinatorStateType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MigrationState} entity type system.
 */
class MigrationStateTest {

    // --- Entity type tests ---

    @Test
    void migrationState_implementsEntityInterface() {
        final MigrationState state = new MigrationState("test-worker");
        assertTrue(state instanceof EntityDAO.Entity, "MigrationState should implement EntityDAO.Entity");
    }

    @Test
    void migrationState_extendsCoordinatorState() {
        final MigrationState state = new MigrationState("test-worker");
        assertTrue(state instanceof CoordinatorState, "MigrationState should extend CoordinatorState");
    }

    @Test
    void getEntityType_returnsClientVersionMigration() {
        final MigrationState state = new MigrationState("test-worker");
        assertNotNull(state.getEntityType());
        assertEquals(EntityType.CLIENT_VERSION_MIGRATION, state.getEntityType());
    }

    @Test
    void getEntityType_ddbValue_isCLIENT_VERSION_MIGRATION() {
        final MigrationState state = new MigrationState("test-worker");
        assertEquals("CLIENT_VERSION_MIGRATION", state.getEntityType().getDdbValue());
    }

    @Test
    void coordinatorStateEntityType_isClientVersionMigration() {
        final MigrationState state = new MigrationState("test-worker");
        assertEquals(CoordinatorStateType.CLIENT_VERSION_MIGRATION, state.getCoordinatorStateEntityType());
    }

    // --- Serialization ---

    @Test
    void testSerialize() {
        final MigrationState state = new MigrationState("worker-1");
        final HashMap<String, AttributeValue> serialized = state.serialize();

        assertTrue(serialized.containsKey(MigrationState.CLIENT_VERSION_ATTRIBUTE_NAME));
        assertEquals(
                ClientVersion.CLIENT_VERSION_INIT.name(),
                serialized.get(MigrationState.CLIENT_VERSION_ATTRIBUTE_NAME).s());

        assertTrue(serialized.containsKey(MigrationState.MODIFIED_BY_ATTRIBUTE_NAME));
        assertEquals(
                "worker-1",
                serialized.get(MigrationState.MODIFIED_BY_ATTRIBUTE_NAME).s());

        assertTrue(serialized.containsKey(MigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME));
        assertNotNull(
                serialized.get(MigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME).n());

        assertTrue(serialized.containsKey("entityType"));
        assertEquals("CLIENT_VERSION_MIGRATION", serialized.get("entityType").s());
    }

    // --- Deserialization ---
    // Note: entityType attribute is stripped by the DAO delegate's resolveEntityType() before
    // calling the deserializer, so deserialize tests should NOT include entityType in attributes.

    @Test
    void deserialize_validKey_preservesEntityType() {
        final HashMap<String, AttributeValue> attributes = new HashMap<>();
        attributes.put(
                MigrationState.CLIENT_VERSION_ATTRIBUTE_NAME,
                AttributeValue.fromS(ClientVersion.CLIENT_VERSION_INIT.name()));
        attributes.put(MigrationState.MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS("worker-1"));
        attributes.put(MigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN("1234567890"));

        final MigrationState deserialized = MigrationState.deserialize(MigrationState.MIGRATION_HASH_KEY, attributes);

        assertNotNull(deserialized);
        assertEquals(ClientVersion.CLIENT_VERSION_INIT, deserialized.getClientVersion());
        assertEquals("worker-1", deserialized.getModifiedBy());
        assertEquals(1234567890L, deserialized.getModifiedTimestamp());
        // entityType is always set by the constructor, not from the attributes map
        assertEquals(EntityType.CLIENT_VERSION_MIGRATION, deserialized.getEntityType());
        assertEquals("CLIENT_VERSION_MIGRATION", deserialized.getEntityType().getDdbValue());
    }

    @Test
    void deserialize_invalidKey_returnsNull() {
        final HashMap<String, AttributeValue> attributes = new HashMap<>();
        attributes.put(
                MigrationState.CLIENT_VERSION_ATTRIBUTE_NAME,
                AttributeValue.fromS(ClientVersion.CLIENT_VERSION_INIT.name()));
        attributes.put(MigrationState.MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS("worker-1"));
        attributes.put(MigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN("1234567890"));

        final MigrationState deserialized = MigrationState.deserialize("wrong-key", attributes);
        assertNull(deserialized);
    }

    @Test
    void deserialize_thenReserialize_includesEntityType() {
        final HashMap<String, AttributeValue> attributes = new HashMap<>();
        attributes.put(
                MigrationState.CLIENT_VERSION_ATTRIBUTE_NAME,
                AttributeValue.fromS(ClientVersion.CLIENT_VERSION_2X.name()));
        attributes.put(MigrationState.MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS("old-worker"));
        attributes.put(MigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN("1000000000"));

        final MigrationState deserialized = MigrationState.deserialize(MigrationState.MIGRATION_HASH_KEY, attributes);

        assertNotNull(deserialized);
        assertEquals(EntityType.CLIENT_VERSION_MIGRATION, deserialized.getEntityType());
        assertEquals(CoordinatorStateType.CLIENT_VERSION_MIGRATION, deserialized.getCoordinatorStateEntityType());

        // Re-serializing will include the entityType attribute (via super.serialize())
        final Map<String, AttributeValue> reserialized = deserialized.serialize();
        assertTrue(reserialized.containsKey("entityType"), "Re-serialized record should include entityType attribute");
        assertEquals("CLIENT_VERSION_MIGRATION", reserialized.get("entityType").s());
    }

    // --- Update preserves entityType ---

    @Test
    void update_preservesEntityType() {
        final MigrationState state = new MigrationState("worker-1");
        state.update(ClientVersion.CLIENT_VERSION_3X, "worker-2");

        assertEquals(EntityType.CLIENT_VERSION_MIGRATION, state.getEntityType());
        assertEquals(ClientVersion.CLIENT_VERSION_3X, state.getClientVersion());
    }

    // --- getDynamoUpdate ---

    @Test
    void getDynamoUpdate_containsExpectedAttributes() {
        final MigrationState state = new MigrationState("worker-1");
        final Map<String, AttributeValueUpdate> updates = state.getDynamoUpdate();

        assertTrue(updates.containsKey(MigrationState.CLIENT_VERSION_ATTRIBUTE_NAME));
        assertTrue(updates.containsKey(MigrationState.MODIFIED_BY_ATTRIBUTE_NAME));
        assertTrue(updates.containsKey(MigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME));
    }

    // --- Copy preserves entityType ---

    @Test
    void copy_preservesEntityType() {
        final MigrationState original = new MigrationState("worker-1");
        final MigrationState copy = original.copy();

        assertEquals(original.getEntityType(), copy.getEntityType());
        assertEquals(EntityType.CLIENT_VERSION_MIGRATION, copy.getEntityType());
    }

    // --- Default client version ---

    @Test
    void newMigrationState_hasInitClientVersion() {
        final MigrationState state = new MigrationState("worker-1");
        assertEquals(ClientVersion.CLIENT_VERSION_INIT, state.getClientVersion());
    }
}
