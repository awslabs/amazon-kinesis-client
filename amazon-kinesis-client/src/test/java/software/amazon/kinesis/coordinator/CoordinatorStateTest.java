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
package software.amazon.kinesis.coordinator;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.kinesis.coordinator.migration.MigrationState;
import software.amazon.kinesis.coordinator.migration.TableMigrationState;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfo;
import software.amazon.kinesis.leader.LeaderLock;
import software.amazon.kinesis.leases.EntityDAO;
import software.amazon.kinesis.leases.EntityType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorStateTest {

    @Test
    void implementsEntityInterface() {
        final CoordinatorState state = new CoordinatorState();
        assertTrue(state instanceof EntityDAO.Entity, "CoordinatorState should implement EntityDAO.Entity");
    }

    @Test
    void getEntityType_withCoordinatorStateType_returnsParentEntityType() {
        final CoordinatorState state = new LeaderLock();

        assertEquals(EntityType.LEADER_LOCK, state.getEntityType());
    }

    @Test
    void getEntityType_withStreamInfoType_returnsStreamInfoEntityType() {
        final CoordinatorState state = new StreamInfo("Stream1", "Stream1-Id");

        assertEquals(EntityType.STREAM_INFO, state.getEntityType());
    }

    @Test
    void getEntityType_withClientVersionMigrationType_returnsCorrectType() {
        final MigrationState state = new MigrationState("WorkerId");

        assertEquals(EntityType.CLIENT_VERSION_MIGRATION, state.getEntityType());
    }

    @Test
    void getEntityType_withTableMigrationType_returnsCorrectType() {
        final CoordinatorState state = new TableMigrationState("Worker");

        assertEquals(EntityType.TABLE_MIGRATION, state.getEntityType());
    }

    @Test
    void getEntityType_withNullCoordinatorStateType_returnsNull() {
        final CoordinatorState state = CoordinatorState.builder().key("someKey").build();

        assertNull(state.getEntityType());
    }

    @Test
    void serialize_includesEntityTypeAttribute() {
        final LeaderLock state = new LeaderLock();

        final Map<String, AttributeValue> serialized = state.serialize();

        assertTrue(serialized.containsKey("entityType"));
        assertEquals("LEADER", serialized.get("entityType").s());
    }

    @Test
    void serialize_includesAttributes() {
        final Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put("customAttr", AttributeValue.fromS("customValue"));
        attributes.put("anotherAttr", AttributeValue.fromN("42"));

        final CoordinatorState state = CoordinatorState.builder()
                .coordinatorStateEntityType(EntityType.CoordinatorStateType.STREAM_INFO)
                .key("streamKey")
                .attributes(attributes)
                .build();

        final Map<String, AttributeValue> serialized = state.serialize();

        assertEquals("STREAM", serialized.get("entityType").s());
        assertEquals("customValue", serialized.get("customAttr").s());
        assertEquals("42", serialized.get("anotherAttr").n());
    }

    @Test
    void serialize_withNullAttributes_returnsEntityType() {
        final CoordinatorState state = CoordinatorState.builder()
                .coordinatorStateEntityType(EntityType.CoordinatorStateType.TABLE_MIGRATION)
                .key("migKey")
                .build();

        final Map<String, AttributeValue> serialized = state.serialize();

        assertEquals(1, serialized.size());
        assertEquals("TABLE_MIGRATION", serialized.get("entityType").s());
    }

    @Test
    void getDynamoUpdate_convertsAttributesToPutUpdates() {
        final Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put("field1", AttributeValue.fromS("val1"));
        attributes.put("field2", AttributeValue.fromN("123"));

        final CoordinatorState state = CoordinatorState.builder()
                .coordinatorStateEntityType(EntityType.CoordinatorStateType.LEADER_LOCK)
                .key(LeaderLock.LEADER_HASH_KEY)
                .attributes(attributes)
                .build();

        final Map<String, AttributeValueUpdate> updates = state.getDynamoUpdate();

        assertNotNull(updates);
        assertEquals(2, updates.size());
        assertEquals(AttributeAction.PUT, updates.get("field1").action());
        assertEquals("val1", updates.get("field1").value().s());
        assertEquals(AttributeAction.PUT, updates.get("field2").action());
        assertEquals("123", updates.get("field2").value().n());
    }

    @Test
    void getDynamoUpdate_withNullAttributes_returnsEmptyMap() {
        final CoordinatorState state = CoordinatorState.builder()
                .coordinatorStateEntityType(EntityType.CoordinatorStateType.LEADER_LOCK)
                .key(LeaderLock.LEADER_HASH_KEY)
                .build();

        final Map<String, AttributeValueUpdate> updates = state.getDynamoUpdate();

        assertNotNull(updates);
        assertTrue(updates.isEmpty());
    }

    @Test
    void constants_areCorrectlyDefined() {
        assertEquals("Leader", LeaderLock.LEADER_HASH_KEY);
    }
}
