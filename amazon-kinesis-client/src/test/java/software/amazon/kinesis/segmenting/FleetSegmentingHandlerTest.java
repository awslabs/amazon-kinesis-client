package software.amazon.kinesis.segmenting;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.LeaseAssignmentStrategy;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FleetSegmentingHandlerTest {

    private DynamoDbClient mockDdbClient;
    private FleetSegmentingHandler handler;
    private String sampleVersionHash;
    private LeaseManagementConfig config;
    private final String tableName = "dummyTableName";

    @BeforeEach
    void setup() {
        clearInvocations();
        mockDdbClient = mock(DynamoDbClient.class);
        sampleVersionHash = String.valueOf(
                LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE.name().hashCode());
        config = new LeaseManagementConfig(
                tableName,
                "dummyApplicationName",
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(KinesisAsyncClient.class),
                "dummyWorkerId");
        config.enableSafeMigrationSystem(true);
        handler = new FleetSegmentingHandler(config, mockDdbClient, tableName);
    }

    @Test
    void getVersionHash_returnsDeterministicValue() {
        String expected = String.valueOf("WORKER_UTILIZATION_AWARE".hashCode());
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
        item.put("versionHash", AttributeValue.fromS(sampleVersionHash));
        item.put(
                "versionHashLut",
                AttributeValue.fromS(String.valueOf(Instant.now().getEpochSecond())));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashKeyMissing() {
        Map<String, AttributeValue> currentVersionItem = new HashMap<>();
        currentVersionItem.put("versionHash", AttributeValue.fromS("differentHash"));
        currentVersionItem.put(
                "versionHashLut",
                AttributeValue.fromS(String.valueOf(Instant.now().getEpochSecond())));
        when(mockDdbClient.getItem(createGetItemRequestForCurrentVersion()))
                .thenReturn(GetItemResponse.builder().item(currentVersionItem).build());

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashDoesNotMatch() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS("differentHash"));
        item.put(
                "versionHashLut",
                AttributeValue.fromS(String.valueOf(Instant.now().getEpochSecond())));
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
    void isOnDeployingVersion_returnsFalse_whenNoDeployingLeaderItem() {
        when(mockDdbClient.getItem(createGetItemRequestForDeployingLeader()))
                .thenReturn(GetItemResponse.builder().build());
        assertFalse(handler.isOnDeployingVersion());
    }

    @Test
    void getVersionHashWithLastUpdatedTime_returnsCorrectMap() {
        final long before = Instant.now().getEpochSecond();
        final Map<String, String> result = handler.getVersionHashWithLastUpdatedTime();
        final long after = Instant.now().getEpochSecond();

        assertTrue(result.containsKey("versionHash"));
        assertTrue(result.containsKey("versionHashLut"));
        assertEquals(sampleVersionHash, result.get("versionHash"));
        final long lut = Long.parseLong(result.get("versionHashLut"));
        assertTrue(lut >= before && lut <= after);
        assertEquals(2, result.size());
    }

    @Test
    void getVersionHashWithLastUpdatedTimeForLockTable_returnsAttributeValueMap() {
        final long before = Instant.now().getEpochSecond();
        final Map<String, AttributeValue> result = handler.getVersionHashWithLastUpdatedTimeForLockTable();
        final long after = Instant.now().getEpochSecond();

        assertEquals(sampleVersionHash, result.get("versionHash").s());
        final long lut = Long.parseLong(result.get("versionHashLut").s());
        assertTrue(lut >= before && lut <= after);
        assertEquals(2, result.size());
    }

    @Test
    void updateCurrentVersion_putsItemWhenAllWorkersEmitting() {
        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        handler.setIsVersionEmittedByAllActiveWorkers(Collections.singletonList(w1), Collections.singletonList(w1));

        handler.updateCurrentVersion();

        verify(mockDdbClient).putItem(any(PutItemRequest.class));
    }

    @Test
    void updateCurrentVersion_doesNotPutItemWhenNotAllWorkersEmitting() {
        handler.updateCurrentVersion();

        verify(mockDdbClient, never()).putItem(any(PutItemRequest.class));
    }

    @Test
    void isWorkerVersionHashStale_returnsTrue_whenLutKeyMissing() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        when(mockWorker.getProperties()).thenReturn(Collections.singletonMap("versionHash", "123"));
        assertTrue(handler.isWorkerVersionHashStale(mockWorker));
    }

    @Test
    void isWorkerVersionHashStale_returnsFalse_whenLutIsRecent() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        Map<String, String> props = new HashMap<>();
        props.put("versionHashLut", String.valueOf(Instant.now().getEpochSecond()));
        when(mockWorker.getProperties()).thenReturn(props);
        assertFalse(handler.isWorkerVersionHashStale(mockWorker));
    }

    @Test
    void isWorkerVersionHashStale_returnsTrue_whenLutIsOlderThanOneHour() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        Map<String, String> props = new HashMap<>();
        props.put("versionHashLut", String.valueOf(Instant.now().getEpochSecond() - 3601));
        when(mockWorker.getProperties()).thenReturn(props);
        assertTrue(handler.isWorkerVersionHashStale(mockWorker));
    }

    @Test
    void isWorkerVersionHashStale_returnsTrue_whenPropertiesNull() {
        WorkerMetricStats worker = mock(WorkerMetricStats.class);
        when(worker.getProperties()).thenReturn(null);
        assertTrue(handler.isWorkerVersionHashStale(worker));
    }

    @Test
    void setIsVersionEmittedByAllActiveWorkers_setsTrue_whenSameWorkerIds() {
        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        WorkerMetricStats w2 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        when(w2.getWorkerId()).thenReturn("worker2");
        List<WorkerMetricStats> active = Arrays.asList(w1, w2);
        List<WorkerMetricStats> onVersion = Arrays.asList(w1, w2);
        handler.setIsVersionEmittedByAllActiveWorkers(active, onVersion);

        assertTrue(handler.isVersionEmittedByAllActiveWorkers());
    }

    @Test
    void setIsVersionEmittedByAllActiveWorkers_setsFalse_whenDifferentWorkerIds() {
        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        WorkerMetricStats w2 = mock(WorkerMetricStats.class);
        WorkerMetricStats w3 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        when(w2.getWorkerId()).thenReturn("worker2");
        when(w3.getWorkerId()).thenReturn("worker1");
        List<WorkerMetricStats> active = Arrays.asList(w1, w2);
        List<WorkerMetricStats> onVersion = Collections.singletonList(w3);
        handler.setIsVersionEmittedByAllActiveWorkers(active, onVersion);

        assertFalse(handler.isVersionEmittedByAllActiveWorkers());
    }

    @Test
    void isEnabled_returnsTrue_whenConfigEnabled() {
        assertTrue(handler.isEnabled());
    }

    @Test
    void isEnabled_returnsFalse_whenConfigDisabled() {
        config.enableSafeMigrationSystem(false);
        FleetSegmentingHandler disabledHandler = new FleetSegmentingHandler(config, mockDdbClient, tableName);
        assertFalse(disabledHandler.isEnabled());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenDisabled() {
        config.enableSafeMigrationSystem(false);
        FleetSegmentingHandler disabledHandler = new FleetSegmentingHandler(config, mockDdbClient, tableName);

        // Even with a CurrentVersion item that has a different hash, disabled handler returns Leader
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS("differentHash"));
        item.put(
                "versionHashLut",
                AttributeValue.fromS(String.valueOf(Instant.now().getEpochSecond())));
        when(mockDdbClient.getItem(createGetItemRequestForCurrentVersion()))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, disabledHandler.getHashKeyForLeaderLock());
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

    private GetItemRequest createGetItemRequestForCurrentVersion() {
        return GetItemRequest.builder()
                .tableName(tableName)
                .key(Collections.singletonMap("key", AttributeValue.fromS("CurrentVersion")))
                .build();
    }
}
