package software.amazon.kinesis.segmenting;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leader.LeaderLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderVersionStateTest {

    private static final String KEY = LeaderVersionState.LEADER_VERSION_HASH_KEY;

    @Test
    void versionKeyForLock_mapsEachLeaderLock() {
        assertEquals(
                LeaderVersionState.LEADER_VERSION_HASH_KEY,
                LeaderVersionState.versionKeyForLock(LeaderLock.LEADER_HASH_KEY));
        assertEquals(
                LeaderVersionState.DEPLOYING_LEADER_VERSION_HASH_KEY,
                LeaderVersionState.versionKeyForLock(LeaderLock.DEPLOYING_LEADER_HASH_KEY));
        assertThrows(IllegalArgumentException.class, () -> LeaderVersionState.versionKeyForLock("SomethingElse"));
    }

    @Test
    void serialize_writesSchemaAttributes() {
        final Map<String, AttributeValue> item = new LeaderVersionState(KEY, "123", "worker-1", 1000L).serialize();

        assertEquals(
                "LEADER", item.get(CoordinatorState.ENTITY_TYPE_ATTRIBUTE_NAME).s());
        assertEquals(
                "123", item.get(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME).s());
        assertEquals(
                "worker-1",
                item.get(LeaderVersionState.OWNER_NAME_ATTRIBUTE_NAME).s());
        assertEquals(
                "1000", item.get(LeaderVersionState.HEARTBEAT_ATTRIBUTE_NAME).n());
        assertEquals(4, item.size());
    }

    @Test
    void deserialize_roundTrips() {
        final LeaderVersionState original = new LeaderVersionState(KEY, "123", "worker-1", 1000L);

        final LeaderVersionState read = LeaderVersionState.deserialize(KEY, original.serialize());

        assertEquals(KEY, read.getKey());
        assertEquals("123", read.getVersionHash());
        assertEquals("worker-1", read.getOwnerName());
        assertEquals(1000L, read.getHeartbeatMillis());
        assertTrue(read.getAttributes().isEmpty());
    }

    @Test
    void deserialize_returnsNullForNonVersionKeys() {
        assertNull(LeaderVersionState.deserialize(LeaderLock.LEADER_HASH_KEY, new HashMap<>()));
        assertNull(LeaderVersionState.deserialize(LeaderLock.DEPLOYING_LEADER_HASH_KEY, new HashMap<>()));
    }

    @Test
    void deserialize_missingHeartbeat_isExpired() {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME, AttributeValue.fromS("123"));

        final LeaderVersionState read = LeaderVersionState.deserialize(KEY, attrs);

        assertNull(read.getHeartbeatMillis());
        assertNull(read.getOwnerName());
        assertTrue(read.isExpired(0L, Long.MAX_VALUE));
    }

    @Test
    void deserialize_unparseableHeartbeat_isExpired() {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME, AttributeValue.fromS("123"));
        attrs.put(LeaderVersionState.HEARTBEAT_ATTRIBUTE_NAME, AttributeValue.fromN("not-a-number"));

        assertTrue(LeaderVersionState.deserialize(KEY, attrs).isExpired(0L, Long.MAX_VALUE));
    }

    @Test
    void deserialize_keepsUnknownAttributes() {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("somethingNew", AttributeValue.fromS("x"));

        assertEquals(
                AttributeValue.fromS("x"),
                LeaderVersionState.deserialize(KEY, attrs).getAttributes().get("somethingNew"));
    }

    @Test
    void isExpired_comparesHeartbeatAgainstExpiry() {
        final LeaderVersionState state = new LeaderVersionState(KEY, "123", "worker-1", 10_000L);

        assertFalse(state.isExpired(15_000L, 5_000L));
        assertTrue(state.isExpired(15_001L, 5_000L));
    }

    @Test
    void getDynamoUpdate_putsSchemaAttributes() {
        final Map<String, AttributeValueUpdate> updates =
                new LeaderVersionState(KEY, "123", "worker-1", 1000L).getDynamoUpdate();

        assertEquals(
                "LEADER",
                updates.get(CoordinatorState.ENTITY_TYPE_ATTRIBUTE_NAME).value().s());
        assertEquals(
                "123",
                updates.get(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME)
                        .value()
                        .s());
        assertEquals(
                "worker-1",
                updates.get(LeaderVersionState.OWNER_NAME_ATTRIBUTE_NAME)
                        .value()
                        .s());
        assertEquals(
                "1000",
                updates.get(LeaderVersionState.HEARTBEAT_ATTRIBUTE_NAME).value().n());
        updates.values().forEach(update -> assertEquals(AttributeAction.PUT, update.action()));
        assertEquals(4, updates.size());
    }
}
