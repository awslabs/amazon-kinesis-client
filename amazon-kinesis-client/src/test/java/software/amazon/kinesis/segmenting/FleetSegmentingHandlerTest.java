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
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.LeaseAssignmentStrategy;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FleetSegmentingHandlerTest {

    private DynamoDbClient mockDdbClient;
    private CoordinatorStateDAO mockCoordinatorStateDAO;
    private FleetSegmentingHandler handler;
    private String sampleVersionHash;
    private LeaseManagementConfig config;
    private final String tableName = "dummyTableName";

    @BeforeEach
    void setup() {
        clearInvocations();
        mockDdbClient = mock(DynamoDbClient.class);
        mockCoordinatorStateDAO = mock(CoordinatorStateDAO.class);
        sampleVersionHash = String.valueOf(LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE.getVersionNum());
        config = new LeaseManagementConfig(
                tableName,
                "dummyApplicationName",
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(KinesisAsyncClient.class),
                "dummyWorkerId");
        config.enableSafeMigrationSystem(true);
        handler = new FleetSegmentingHandler(config, mockDdbClient, tableName, mockCoordinatorStateDAO);
    }

    @Test
    void getVersionHash_returnsDeterministicValue() {
        String expected = String.valueOf(LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE.getVersionNum());
        assertEquals(expected, handler.getVersionHash());
    }

    @Test
    void getVersionHash_isSameAcrossInstancesForSameConfig() {
        FleetSegmentingHandler handler2 =
                new FleetSegmentingHandler(config, mockDdbClient, tableName, mockCoordinatorStateDAO);
        assertEquals(handler.getVersionHash(), handler2.getVersionHash());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashMatches() throws Exception {
        mockCoordinatorState(
                CoordinatorState.LEADER_HASH_KEY,
                sampleVersionHash,
                Instant.now().getEpochSecond());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashKeyMissing() throws Exception {
        mockCoordinatorState(
                CoordinatorState.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashDoesNotMatch() throws Exception {
        mockCoordinatorState(
                CoordinatorState.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenItemNotPresent() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(CoordinatorState.LEADER_HASH_KEY))
                .thenReturn(null);

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashExpired() throws Exception {
        mockCoordinatorState(
                CoordinatorState.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 7200);

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void isOnCurrentVersion_returnsTrue_whenVersionHashMatches() throws Exception {
        mockCoordinatorState(
                CoordinatorState.LEADER_HASH_KEY,
                sampleVersionHash,
                Instant.now().getEpochSecond());
        assertTrue(handler.isOnCurrentVersion());
    }

    @Test
    void isOnCurrentVersion_returnsFalse_whenVersionHashDoesNotMatch() throws Exception {
        mockCoordinatorState(
                CoordinatorState.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        assertFalse(handler.isOnCurrentVersion());
    }

    @Test
    void isOnCurrentVersion_returnsFalse_whenItemNotPresent() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(CoordinatorState.LEADER_HASH_KEY))
                .thenReturn(null);
        assertFalse(handler.isOnCurrentVersion());
    }

    @Test
    void isOnCurrentVersion_returnsFalse_whenVersionHashKeyMissing() throws Exception {
        Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("someOtherKey", AttributeValue.fromS("value"));
        CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(CoordinatorState.LEADER_HASH_KEY))
                .thenReturn(state);

        assertFalse(handler.isOnCurrentVersion());
    }

    @Test
    void isOnDeployingVersion_returnsTrue_whenVersionHashMatches() throws Exception {
        mockCoordinatorState(
                CoordinatorState.DEPLOYING_LEADER_HASH_KEY,
                sampleVersionHash,
                Instant.now().getEpochSecond());
        assertTrue(handler.isOnDeployingVersion());
    }

    @Test
    void isOnDeployingVersion_returnsFalse_whenVersionHashDoesNotMatch() throws Exception {
        mockCoordinatorState(
                CoordinatorState.DEPLOYING_LEADER_HASH_KEY,
                "differentHash",
                Instant.now().getEpochSecond());
        assertFalse(handler.isOnDeployingVersion());
    }

    @Test
    void isOnDeployingVersion_returnsFalse_whenNoDeployingLeaderItem() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(CoordinatorState.DEPLOYING_LEADER_HASH_KEY))
                .thenReturn(null);
        assertFalse(handler.isOnDeployingVersion());
    }

    @Test
    void getVersionHashWithLastUpdatedTime_returnsCorrectMap() {
        final long before = Instant.now().getEpochSecond();
        final Map<String, String> result = handler.getVersionHashWithLastUpdatedTime();
        final long after = Instant.now().getEpochSecond();

        assertTrue(result.containsKey("VersionHash"));
        assertTrue(result.containsKey("VersionHashLut"));
        assertEquals(sampleVersionHash, result.get("VersionHash"));
        final long lut = Long.parseLong(result.get("VersionHashLut"));
        assertTrue(lut >= before && lut <= after);
        assertEquals(2, result.size());
    }

    @Test
    void getVersionHashWithLastUpdatedTimeForLockTable_returnsAttributeValueMap() {
        final long before = Instant.now().getEpochSecond();
        final Map<String, AttributeValue> result = handler.getVersionHashWithLastUpdatedTimeForLockTable();
        final long after = Instant.now().getEpochSecond();

        assertEquals(sampleVersionHash, result.get("VersionHash").s());
        final long lut = Long.parseLong(result.get("VersionHashLut").s());
        assertTrue(lut >= before && lut <= after);
        assertEquals(2, result.size());
    }

    @Test
    void isWorkerVersionHashStale_returnsTrue_whenLutKeyMissing() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        when(mockWorker.getProperties()).thenReturn(Collections.singletonMap("VersionHash", "123"));
        assertTrue(handler.isWorkerVersionHashStale(mockWorker));
    }

    @Test
    void isWorkerVersionHashStale_returnsFalse_whenLutIsRecent() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        Map<String, String> props = new HashMap<>();
        props.put("VersionHashLut", String.valueOf(Instant.now().getEpochSecond()));
        when(mockWorker.getProperties()).thenReturn(props);
        assertFalse(handler.isWorkerVersionHashStale(mockWorker));
    }

    @Test
    void isWorkerVersionHashStale_returnsTrue_whenLutIsOlderThanOneHour() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        Map<String, String> props = new HashMap<>();
        props.put("VersionHashLut", String.valueOf(Instant.now().getEpochSecond() - 3601));
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
        WorkerMetricStats w4 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        when(w2.getWorkerId()).thenReturn("worker2");
        when(w3.getWorkerId()).thenReturn("worker3");
        when(w4.getWorkerId()).thenReturn("worker4");
        List<WorkerMetricStats> active = Arrays.asList(w1, w2);
        List<WorkerMetricStats> onVersion = Arrays.asList(w3, w4);
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
        FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockDdbClient, tableName, mockCoordinatorStateDAO);
        assertFalse(disabledHandler.isEnabled());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenDisabled() throws Exception {
        config.enableSafeMigrationSystem(false);
        FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockDdbClient, tableName, mockCoordinatorStateDAO);

        // Even with a CurrentVersion item that has a different hash, disabled handler returns Leader
        mockCoordinatorState(
                CoordinatorState.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, disabledHandler.getHashKeyForLeaderLock());
    }

    private void mockCoordinatorState(String key, String versionHash, long lut) throws Exception {
        Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("VersionHash", AttributeValue.fromS(versionHash));
        attrs.put("VersionHashLut", AttributeValue.fromS(String.valueOf(lut)));
        CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(key))).thenReturn(state);
    }
}
