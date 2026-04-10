package software.amazon.kinesis.segmenting;

import java.util.Arrays;
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
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FleetSegmentingHandlerTest {

    private DynamoDbClient mockDdbClient;
    private WorkerMetricStatsDAO mockWorkerMetricStatsDAO;
    private FleetSegmentingHandler handler;
    private String sampleVersionHash;
    private LeaseManagementConfig config;
    private final String tableName = "dummyTableName";

    @BeforeEach
    void setup() {
        clearInvocations();
        mockDdbClient = mock(DynamoDbClient.class);
        mockWorkerMetricStatsDAO = mock(WorkerMetricStatsDAO.class);
        sampleVersionHash = String.valueOf(LeaseAssignmentMetric.CPU.name().hashCode());
        config = new LeaseManagementConfig(
                tableName,
                "dummyApplicationName",
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(KinesisAsyncClient.class),
                "dummyWorkerId",
                LeaseAssignmentMetric.CPU);
        handler = new FleetSegmentingHandler(config, mockDdbClient, tableName, mockWorkerMetricStatsDAO);
    }

    @Test
    void getVersionHash_returnsDeterministicValue() {
        String expected = String.valueOf("CPU".hashCode());
        assertEquals(expected, handler.getVersionHash());
    }

    @Test
    void getVersionHash_isSameAcrossInstancesForSameConfig() {
        FleetSegmentingHandler handler2 =
                new FleetSegmentingHandler(config, mockDdbClient, tableName, mockWorkerMetricStatsDAO);
        assertEquals(handler.getVersionHash(), handler2.getVersionHash());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashMatches() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS(sampleVersionHash));
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
    void isOnCurrentVersion_returnsTrue_whenVersionHashMatches() {
        when(mockDdbClient.getItem(createGetItemRequestForCurrentLeader()))
                .thenReturn(createGetItemResponseForCurrentLeader(sampleVersionHash));
        assertTrue(handler.isOnCurrentVersion());
    }

    @Test
    void isOnCurrentVersion_returnsFalse_whenVersionHashDoesNotMatch() {
        when(mockDdbClient.getItem(createGetItemRequestForCurrentLeader()))
                .thenReturn(createGetItemResponseForCurrentLeader("differentHash"));

        assertFalse(handler.isOnCurrentVersion());
    }

    @Test
    void isOnCurrentVersion_returnsFalse_whenItemNotPresent() {
        when(mockDdbClient.getItem(createGetItemRequestForCurrentLeader()))
                .thenReturn(GetItemResponse.builder().build());

        assertFalse(handler.isOnCurrentVersion());
    }

    @Test
    void isOnCurrentVersion_returnsFalse_whenVersionHashKeyMissing() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("someOtherKey", AttributeValue.fromS("value"));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertFalse(handler.isOnCurrentVersion());
    }

    @Test
    void isOnDeployingVersion_returnsTrue_whenVersionHashMatches() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(createGetItemResponseForDeployingLeader(sampleVersionHash));
        assertTrue(handler.isOnDeployingVersion());
    }

    @Test
    void isOnDeployingVersion_returnsFalse_whenVersionHashDoesNotMatch() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(createGetItemResponseForDeployingLeader("differentHash"));
        assertFalse(handler.isOnDeployingVersion());
    }

    @Test
    void isOnDeployingVersion_returnsFalse_whenItemNotPresent() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(GetItemResponse.builder().build());
        assertFalse(handler.isOnDeployingVersion());
    }

    @Test
    void areAllWorkersEmittingDeployingVersion_returnsFalse_whenNotOnDeployingVersion() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(GetItemResponse.builder().build());
        assertFalse(handler.areAllWorkersEmittingDeployingVersion());
    }

    @Test
    void areAllWorkersEmittingDeployingVersion_returnsTrue_whenAllWorkersMatch() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(createGetItemResponseForDeployingLeader(sampleVersionHash));

        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        WorkerMetricStats w2 = mock(WorkerMetricStats.class);
        when(w1.getProperties()).thenReturn(Collections.singletonMap("versionHash", sampleVersionHash));
        when(w2.getProperties()).thenReturn(Collections.singletonMap("versionHash", sampleVersionHash));
        when(mockWorkerMetricStatsDAO.getAllWorkerMetricStats()).thenReturn(Arrays.asList(w1, w2));

        assertTrue(handler.areAllWorkersEmittingDeployingVersion());
    }

    @Test
    void areAllWorkersEmittingDeployingVersion_returnsFalse_whenOneWorkerDiffers() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(createGetItemResponseForDeployingLeader(sampleVersionHash));

        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        WorkerMetricStats w2 = mock(WorkerMetricStats.class);
        when(w1.getProperties()).thenReturn(Collections.singletonMap("versionHash", sampleVersionHash));
        when(w2.getProperties()).thenReturn(Collections.singletonMap("versionHash", "oldVersion"));
        when(mockWorkerMetricStatsDAO.getAllWorkerMetricStats()).thenReturn(Arrays.asList(w1, w2));

        assertFalse(handler.areAllWorkersEmittingDeployingVersion());
    }

    @Test
    void areAllWorkersEmittingDeployingVersion_returnsTrue_whenNoWorkers() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(createGetItemResponseForDeployingLeader(sampleVersionHash));
        when(mockWorkerMetricStatsDAO.getAllWorkerMetricStats()).thenReturn(Collections.emptyList());

        assertTrue(handler.areAllWorkersEmittingDeployingVersion());
    }

    @Test
    void areAllWorkersEmittingDeployingVersion_returnsFalse_whenWorkerMissingVersionHashKey() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(createGetItemResponseForDeployingLeader(sampleVersionHash));

        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        when(w1.getProperties()).thenReturn(Collections.singletonMap("someOtherKey", "value"));
        when(mockWorkerMetricStatsDAO.getAllWorkerMetricStats()).thenReturn(Collections.singletonList(w1));

        assertFalse(handler.areAllWorkersEmittingDeployingVersion());
    }

    @Test
    void getVersionHashAsMap_returnsCorrectMap() {
        assertEquals(Collections.singletonMap("versionHash", sampleVersionHash), handler.getVersionHashAsMap());
    }

    private GetItemRequest createGetItemRequestForDeployingLeader() {
        return GetItemRequest.builder()
                .tableName(tableName)
                .key(Collections.singletonMap("key", AttributeValue.fromS(CoordinatorState.DEPLOYING_LEADER_HASH_KEY)))
                .build();
    }

    private GetItemResponse createGetItemResponseForDeployingLeader(final String versionHash) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("key", AttributeValue.fromS(CoordinatorState.DEPLOYING_LEADER_HASH_KEY));
        item.put("versionHash", AttributeValue.fromS(versionHash));
        return GetItemResponse.builder().item(item).build();
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
