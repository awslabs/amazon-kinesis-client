package software.amazon.kinesis.segmenting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.LeaseAssignmentMetric;
import software.amazon.kinesis.leases.LeaseManagementConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FleetSegmentingHandlerTest {

    private DynamoDbClient mockDdbClient;
    private FleetSegmentingHandler handler;
    private String currentVersionHash;
    private LeaseManagementConfig config;
    private final String tableName = "dummyTableName";

    @BeforeEach
    void setup() {
        clearInvocations();
        mockDdbClient = mock(DynamoDbClient.class);
        currentVersionHash = String.valueOf(LeaseAssignmentMetric.CPU.name().hashCode());
        config = new LeaseManagementConfig(
                tableName,
                "dummyApplicationName",
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(KinesisAsyncClient.class),
                "dummyWorkerId",
                LeaseAssignmentMetric.CPU);
        handler = new FleetSegmentingHandler(config, mockDdbClient, tableName);
    }

    @Test
    void getVersionHash_returnsDeterministicValue() {
        String expected = String.valueOf("CPU".hashCode());
        assertEquals(expected, handler.getVersionHash());
    }

    @Test
    void getVersionHash_isSameAcrossInstancesForSameConfig() {
        FleetSegmentingHandler handler2 = new FleetSegmentingHandler(config, mockDdbClient, tableName);
        assertEquals(handler.getVersionHash(), handler2.getVersionHash());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashMatches() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS(currentVersionHash));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashKeyMissing() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("someOtherKey", AttributeValue.fromS("value"));
        when(mockDdbClient.getItem(createGetItemRequestForCurrentLeader()))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashDoesNotMatch() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS("differentHash"));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenItemNotPresent() {
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().build());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void isWorkerOnCurrentVersion_returnsTrue_whenVersionHashMatches() {
        when(mockDdbClient.getItem(createGetItemRequestForCurrentLeader()))
                .thenReturn(createGetItemResponseForCurrentLeader(currentVersionHash));
        assertTrue(handler.isWorkerOnCurrentVersion());
    }

    @Test
    void isWorkerOnCurrentVersion_returnsFalse_whenVersionHashDoesNotMatch() {
        when(mockDdbClient.getItem(createGetItemRequestForCurrentLeader()))
                .thenReturn(createGetItemResponseForCurrentLeader("differentHash"));

        assertFalse(handler.isWorkerOnCurrentVersion());
    }

    @Test
    void isWorkerOnCurrentVersion_returnsFalse_whenItemNotPresent() {
        when(mockDdbClient.getItem(createGetItemRequestForCurrentLeader()))
                .thenReturn(GetItemResponse.builder().build());

        assertFalse(handler.isWorkerOnCurrentVersion());
    }

    @Test
    void isWorkerOnCurrentVersion_returnsFalse_whenVersionHashKeyMissing() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("someOtherKey", AttributeValue.fromS("value"));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertFalse(handler.isWorkerOnCurrentVersion());
    }

    private GetItemRequest createGetItemRequestForCurrentLeader() {
        return GetItemRequest.builder()
                .tableName(tableName)
                .key(Collections.singletonMap("key", AttributeValue.fromS(CoordinatorState.LEADER_HASH_KEY)))
                .build();
    }

    private GetItemResponse createGetItemResponseForCurrentLeader(final String versionHash) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("key", AttributeValue.fromS(CoordinatorState.LEADER_HASH_KEY));
        item.put("versionHash", AttributeValue.fromS(versionHash));
        return GetItemResponse.builder().item(item).build();
    }
}
