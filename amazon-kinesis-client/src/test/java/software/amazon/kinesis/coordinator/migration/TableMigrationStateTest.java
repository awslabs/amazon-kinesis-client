package software.amazon.kinesis.coordinator.migration;

import java.util.HashMap;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link TableMigrationState} serialization, deserialization, and updates.
 */
class TableMigrationStateTest {

    private static final String WORKER_ID = "test-worker";

    @Test
    void constructor_setsDefaultState_toInit() {
        TableMigrationState state = new TableMigrationState(WORKER_ID);
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, state.getTableMigrationStatus());
        assertEquals(WORKER_ID, state.getModifiedBy());
        assertNotNull(state.getHistory());
        assertTrue(state.getHistory().isEmpty());
    }

    @Test
    void serialize_includesAllFields() {
        TableMigrationState state = new TableMigrationState(WORKER_ID);
        HashMap<String, AttributeValue> serialized = state.serialize();

        assertTrue(serialized.containsKey(TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME));
        assertTrue(serialized.containsKey(TableMigrationState.MODIFIED_BY_ATTRIBUTE_NAME));
        assertTrue(serialized.containsKey(TableMigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME));
        assertEquals(
                TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT.name(),
                serialized
                        .get(TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME)
                        .s());
        assertEquals(
                WORKER_ID,
                serialized.get(TableMigrationState.MODIFIED_BY_ATTRIBUTE_NAME).s());
    }

    @Test
    void deserialize_validAttributes_roundTrips() {
        TableMigrationState original = new TableMigrationState(WORKER_ID);
        HashMap<String, AttributeValue> serialized = original.serialize();

        TableMigrationState deserialized =
                TableMigrationState.deserialize(TableMigrationState.TABLE_MIGRATION_HASH_KEY, serialized);

        assertNotNull(deserialized);
        assertEquals(original.getTableMigrationStatus(), deserialized.getTableMigrationStatus());
        assertEquals(original.getModifiedBy(), deserialized.getModifiedBy());
        assertEquals(original.getModifiedTimestamp(), deserialized.getModifiedTimestamp());
    }

    @Test
    void deserialize_wrongKey_returnsNull() {
        HashMap<String, AttributeValue> attrs = new HashMap<>();
        attrs.put(
                TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME,
                AttributeValue.fromS(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT.name()));
        attrs.put(TableMigrationState.MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS(WORKER_ID));
        attrs.put(TableMigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN("1000"));

        TableMigrationState result = TableMigrationState.deserialize("wrong-key", attrs);
        assertNull(result);
    }

    @Test
    void deserialize_missingRequiredField_returnsNull() {
        HashMap<String, AttributeValue> attrs = new HashMap<>();
        // Missing TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME
        attrs.put(TableMigrationState.MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS(WORKER_ID));
        attrs.put(TableMigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN("1000"));

        TableMigrationState result =
                TableMigrationState.deserialize(TableMigrationState.TABLE_MIGRATION_HASH_KEY, attrs);
        assertNull(result);
    }

    @Test
    void update_changesStatusAndModifiedBy_addsHistory() {
        TableMigrationState state = new TableMigrationState("worker-1");
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, "worker-2");

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, state.getTableMigrationStatus());
        assertEquals("worker-2", state.getModifiedBy());
        assertEquals(1, state.getHistory().size());
    }

    @Test
    void update_multipleTransitions_preservesHistory() {
        TableMigrationState state = new TableMigrationState("worker-1");
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, "worker-2");
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, "worker-3");
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, "worker-4");

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, state.getTableMigrationStatus());
        assertEquals(3, state.getHistory().size());
    }

    @Test
    void serialize_afterUpdate_includesNewState() {
        TableMigrationState state = new TableMigrationState(WORKER_ID);
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, "leader-1");

        HashMap<String, AttributeValue> serialized = state.serialize();
        assertEquals(
                TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED.name(),
                serialized
                        .get(TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME)
                        .s());
    }

    @Test
    void deserialize_withHistory_preservesHistoryEntries() {
        TableMigrationState state = new TableMigrationState("worker-1");
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, "worker-2");
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, "worker-3");

        HashMap<String, AttributeValue> serialized = state.serialize();
        TableMigrationState deserialized =
                TableMigrationState.deserialize(TableMigrationState.TABLE_MIGRATION_HASH_KEY, serialized);

        assertNotNull(deserialized);
        assertEquals(2, deserialized.getHistory().size());
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, deserialized.getTableMigrationStatus());
    }

    @Test
    void allStatusTransitions_initToComplete() {
        TableMigrationState state = new TableMigrationState(WORKER_ID);
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, state.getTableMigrationStatus());

        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, "leader");
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, state.getTableMigrationStatus());

        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, "leader");
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, state.getTableMigrationStatus());

        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, "leader");
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, state.getTableMigrationStatus());
    }

    @Test
    void rollbackTransition_pendingToDeployed() {
        TableMigrationState state = new TableMigrationState(WORKER_ID);
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, "leader");
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, "leader");
        state.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, "leader"); // rollback

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, state.getTableMigrationStatus());
        assertEquals(3, state.getHistory().size());
    }
}
