package software.amazon.kinesis.leader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.coordinator.migration.TableMigrationStateMachine;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.segmenting.FleetSegmentingHandler;
import software.amazon.kinesis.segmenting.LeaderVersionState;
import software.amazon.kinesis.segmenting.VersionStateUnavailableException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamoDBLockBasedLeaderDeciderTest {

    private static final String TEST_LOCK_TABLE_NAME = "IAmTestLockTable";

    private final AmazonDynamoDBLocal dynamoDBEmbedded = DynamoDBEmbedded.create();
    private final DynamoDbAsyncClient dynamoDBAsyncClient = dynamoDBEmbedded.dynamoDbAsyncClient();
    private final DynamoDbClient dynamoDBSyncClient = dynamoDBEmbedded.dynamoDbClient();
    private final Map<String, DynamoDBLockBasedLeaderDecider> workerIdToLeaderDeciderMap = new HashMap<>();
    // To create Lease table
    private final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
            TEST_LOCK_TABLE_NAME,
            dynamoDBAsyncClient,
            new DynamoDBLeaseSerializer(),
            true,
            TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
            LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
            new DdbTableConfig(),
            LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
            LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
            DefaultSdkAutoConstructList.getInstance());

    private TableMigrationStateMachine mockTableMigrationStateMachine;

    private FleetSegmentingHandler mockSegmentingHandler;

    @BeforeEach
    void setup() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        leaseRefresher.createLeaseTableIfNotExists();
        mockTableMigrationStateMachine = mock(TableMigrationStateMachine.class);
        mockSegmentingHandler = mock(FleetSegmentingHandler.class);
        when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(LeaderLock.LEADER_HASH_KEY);
        IntStream.range(0, 10).sequential().forEach(index -> {
            final String workerId = getWorkerId(index);
            workerIdToLeaderDeciderMap.put(workerId, newDecider(workerId, 100L, 10L));
        });

        workerIdToLeaderDeciderMap.values().forEach(DynamoDBLockBasedLeaderDecider::initialize);
    }

    private DynamoDBLockBasedLeaderDecider newDecider(
            final String workerId, final long leaseDurationMillis, final long heartbeatPeriodMillis) {
        return newDecider(workerId, leaseDurationMillis, heartbeatPeriodMillis, new NullMetricsFactory());
    }

    private DynamoDBLockBasedLeaderDecider newDecider(
            final String workerId,
            final long leaseDurationMillis,
            final long heartbeatPeriodMillis,
            final software.amazon.kinesis.metrics.MetricsFactory metricsFactory) {
        return DynamoDBLockBasedLeaderDecider.create(
                newDao(),
                workerId,
                leaseDurationMillis,
                heartbeatPeriodMillis,
                metricsFactory,
                mockTableMigrationStateMachine,
                mockSegmentingHandler);
    }

    private CoordinatorStateDAO newDao() {
        final CoordinatorConfig c = new CoordinatorConfig("TestApplication");
        c.coordinatorStateTableConfig().tableName(TEST_LOCK_TABLE_NAME);
        final TableMigrationStatusProvider provider = mock(TableMigrationStatusProvider.class);
        when(provider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        final CoordinatorStateDAO dao = new CoordinatorStateDAO(
                dynamoDBAsyncClient,
                c.coordinatorStateTableConfig(),
                TEST_LOCK_TABLE_NAME,
                provider,
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT);
        try {
            dao.initialize();
        } catch (final InvalidStateException e) {
            throw new RuntimeException(e);
        }
        return dao;
    }

    private static String getWorkerId(final int index) {
        return "worker" + index;
    }

    /**
     * Read-failure path only: a worker whose version-state read fails and that has never read it
     * successfully sits out one cycle instead of guessing "Leader". It holds nothing and writes nothing, and the same
     * worker contends normally once a read succeeds. (A successful read of a Leader item with no versionHash, as on
     * a first rollout, takes the normal path: see isLeader_firstRollout_updatedWorkersContendForLeader.)
     */
    @Test
    void isLeader_versionStateReadFails_sitsOutOneCycleThenContends() throws InterruptedException {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);
        when(mockSegmentingHandler.getHashKeyForLeaderLock())
                .thenThrow(new VersionStateUnavailableException("read failed", new RuntimeException()));

        assertFalse(decider.isLeader(workerId));
        verify(mockSegmentingHandler).setHeldLeaderKey(null);
        assertTrue(getItem(LeaderLock.LEADER_HASH_KEY).isEmpty());
        assertTrue(getItem(LeaderLock.DEPLOYING_LEADER_HASH_KEY).isEmpty());

        // Reads recover. After the non-leader backoff (the 10ms heartbeat period here) the same worker contends.
        doReturn(LeaderLock.LEADER_HASH_KEY).when(mockSegmentingHandler).getHashKeyForLeaderLock();
        Thread.sleep(20);
        assertTrue(decider.isLeader(workerId));
    }

    /**
     * First rollout of the segmenting handler into a fleet that doesn't have it. The Leader lock is held by a worker
     * without segmenting, so its item has no versionHash. Updated workers must contend for Leader exactly as before:
     * nobody takes DeployingLeader, and an updated worker becomes Leader normally once the old one releases it.
     */
    @Test
    void isLeader_firstRollout_updatedWorkersContendForLeader() throws Exception {
        // Worker without segmenting (the mocked handler is disabled by default) takes Leader.
        final String oldWorkerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider oldDecider = workerIdToLeaderDeciderMap.get(oldWorkerId);
        assertTrue(oldDecider.isLeader(oldWorkerId));
        assertFalse(getLeaderItem().containsKey(FleetSegmentingHandler.VERSION_HASH_KEY));

        // Updated worker with a real, enabled handler reading the same table.
        final String newWorkerId = "updatedWorker";
        final LeaseManagementConfig config = new LeaseManagementConfig(
                "dummyTableName",
                "dummyApplicationName",
                mock(software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient.class),
                mock(software.amazon.awssdk.services.kinesis.KinesisAsyncClient.class),
                newWorkerId);
        config.enableRollingDeploymentSystem(true);
        // The handler and decider share one DAO, as in the Scheduler, so the version DAO sees the decider's locks.
        final CoordinatorStateDAO sharedDao = newDao();
        final FleetSegmentingHandler realHandler =
                new FleetSegmentingHandler(config, sharedDao, inlineExecutor(), () -> true);
        final DynamoDBLockBasedLeaderDecider newDecider = DynamoDBLockBasedLeaderDecider.create(
                sharedDao,
                newWorkerId,
                100L,
                10L,
                new NullMetricsFactory(),
                mockTableMigrationStateMachine,
                realHandler);
        newDecider.initialize();

        // The updated worker picks Leader, not DeployingLeader, and doesn't get it while the old worker holds it.
        assertEquals(LeaderLock.LEADER_HASH_KEY, realHandler.getHashKeyForLeaderLock());
        assertFalse(newDecider.isLeader(newWorkerId));
        assertTrue(getItem(LeaderLock.DEPLOYING_LEADER_HASH_KEY).isEmpty());

        // The old worker shuts down; the updated worker takes Leader and writes its version to the version item.
        oldDecider.shutdown();
        Thread.sleep(20);
        assertTrue(newDecider.isLeader(newWorkerId));
        assertEquals(
                realHandler.getVersionHash(),
                getItem(LeaderVersionState.LEADER_VERSION_HASH_KEY)
                        .get(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME)
                        .s());
        assertFalse(getLeaderItem().containsKey(FleetSegmentingHandler.VERSION_HASH_KEY));
        assertTrue(getItem(LeaderLock.DEPLOYING_LEADER_HASH_KEY).isEmpty());
        newDecider.shutdown();
    }

    @Test
    void isLeader_multipleWorkerTryingLock_assertOnlySingleOneAcquiringLock() {
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        workerIdToLeaderDeciderMap.entrySet().stream().parallel().forEach(entry -> {
            if (entry.getValue().isLeader(entry.getKey())) {
                atomicInteger.incrementAndGet();
            }
        });

        assertEquals(1, atomicInteger.get(), "Multiple workers were able to get lock");
    }

    @Test
    void isLeader_sameWorkerChecksLeadershipSeveralTime_assertTrueInAllCases() {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);
        for (int i = 0; i < 5; ++i) {
            assertTrue(decider.isLeader(workerId));
        }
        decider.shutdown();

        final GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(TEST_LOCK_TABLE_NAME)
                .key(Collections.singletonMap(
                        DynamoDBLeaseSerializer.LEASE_KEY_KEY,
                        AttributeValue.builder().s(LeaderLock.LEADER_HASH_KEY).build()))
                .build();
        final GetItemResponse getItemResult = dynamoDBSyncClient.getItem(getItemRequest);
        // Segmenting is disabled here, so release behaves as before segmenting: the lock item is deleted.
        assertFalse(getItemResult.hasItem());

        // After shutdown, assert that leaderDecider returns false.
        assertFalse(decider.isLeader(workerId), "LeaderDecider did not return false after shutdown.");
    }

    /** The leader lock never carries a version; the version is kept in its own item. */
    @Test
    void isLeader_segmentingEnabled_lockItemHasNoVersionAttributes() {
        final String workerId = getWorkerId(1);
        when(mockSegmentingHandler.isEnabled()).thenReturn(true);

        assertTrue(workerIdToLeaderDeciderMap.get(workerId).isLeader(workerId));

        final Map<String, AttributeValue> lock = getLeaderItem();
        assertFalse(lock.containsKey(FleetSegmentingHandler.VERSION_HASH_KEY));
        assertFalse(lock.containsKey("versionHashLut"));
        verify(mockSegmentingHandler).setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);
    }

    /** Release deletes the lock item as it did before segmenting, since no version is kept on it. */
    @Test
    void shutdown_segmentingEnabled_deletesLockItem() {
        final String workerId = getWorkerId(1);
        when(mockSegmentingHandler.isEnabled()).thenReturn(true);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);
        assertTrue(decider.isLeader(workerId));

        decider.shutdown();

        assertTrue(getLeaderItem().isEmpty());
        verify(mockSegmentingHandler).setHeldLeaderKey(null);
    }

    /** A lock holder answers isLeader from local state and doesn't re-read the key on every call. */
    @Test
    void isLeader_holder_doesNotReevaluateKeyWithinHeartbeatPeriod() {
        final String workerId = "longHeartbeatWorker";
        final DynamoDBLockBasedLeaderDecider decider = newDecider(workerId, 120_000L, 30_000L);
        decider.initialize();
        when(mockSegmentingHandler.isEnabled()).thenReturn(true);

        assertTrue(decider.isLeader(workerId));
        for (int i = 0; i < 5; ++i) {
            assertTrue(decider.isLeader(workerId));
        }

        verify(mockSegmentingHandler, times(1)).getHashKeyForLeaderLock();
        decider.shutdown();
    }

    /** End of a deploy: the deploying leader releases DeployingLeader before taking Leader. */
    @Test
    void isLeader_keyChanges_releasesHeldLockThenAcquiresNewKey() throws InterruptedException {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);
        when(mockSegmentingHandler.isEnabled()).thenReturn(true);
        when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        assertTrue(decider.isLeader(workerId));
        verify(mockSegmentingHandler).setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);

        // Past the 10ms heartbeat period, so the key is re-evaluated.
        Thread.sleep(20);
        when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(LeaderLock.LEADER_HASH_KEY);
        assertTrue(decider.isLeader(workerId));

        assertTrue(getItem(LeaderLock.DEPLOYING_LEADER_HASH_KEY).isEmpty(), "DeployingLeader lock was not released");
        verify(mockSegmentingHandler).setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);
    }

    /** The decider reports whether this worker is the DeployingLeader. */
    @Test
    void isLeader_deployingLeader_emitsIsDeployingLeaderMetric() {
        final software.amazon.kinesis.metrics.MetricsFactory factory =
                mock(software.amazon.kinesis.metrics.MetricsFactory.class);
        final software.amazon.kinesis.metrics.MetricsScope scope =
                mock(software.amazon.kinesis.metrics.MetricsScope.class);
        when(factory.createMetrics()).thenReturn(scope);
        final String workerId = "metricsWorker";
        final DynamoDBLockBasedLeaderDecider decider = newDecider(workerId, 100L, 10L, factory);
        decider.initialize();
        when(mockSegmentingHandler.isEnabled()).thenReturn(true);
        when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(LeaderLock.DEPLOYING_LEADER_HASH_KEY);

        assertTrue(decider.isLeader(workerId));

        verify(scope)
                .addData(eq(LeaderDecider.METRIC_OPERATION_LEADER_DECIDER_IS_DEPLOYING_LEADER), eq(1.0), any(), any());
        decider.shutdown();
    }

    @Test
    void isLeader_segmentingDisabled_doesNotEmitIsDeployingLeaderMetric() {
        final software.amazon.kinesis.metrics.MetricsFactory factory =
                mock(software.amazon.kinesis.metrics.MetricsFactory.class);
        final software.amazon.kinesis.metrics.MetricsScope scope =
                mock(software.amazon.kinesis.metrics.MetricsScope.class);
        when(factory.createMetrics()).thenReturn(scope);
        final String workerId = "metricsWorker";
        final DynamoDBLockBasedLeaderDecider decider = newDecider(workerId, 100L, 10L, factory);
        decider.initialize();

        assertTrue(decider.isLeader(workerId));

        verify(scope, org.mockito.Mockito.never())
                .addData(
                        eq(LeaderDecider.METRIC_OPERATION_LEADER_DECIDER_IS_DEPLOYING_LEADER),
                        org.mockito.ArgumentMatchers.anyDouble(),
                        any(),
                        any());
        decider.shutdown();
    }

    /** Only the Leader holder drives table migration; the DeployingLeader reports false. */
    @Test
    void isLeader_deployingLeader_reportsNotLeaderToTableMigration() throws Exception {
        final String workerId = getWorkerId(1);
        when(mockSegmentingHandler.isEnabled()).thenReturn(true);
        when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(LeaderLock.DEPLOYING_LEADER_HASH_KEY);

        assertTrue(workerIdToLeaderDeciderMap.get(workerId).isLeader(workerId));

        verify(mockTableMigrationStateMachine).handleLeaderLockResult(false);
        verify(mockTableMigrationStateMachine, org.mockito.Mockito.never()).handleLeaderLockResult(true);
    }

    /**
     * Shutdown releases the lock this worker actually holds, even if the handler would now name the other key.
     * Before, it released only the key the handler returned, which left a held DeployingLeader lock until expiry.
     */
    @Test
    void shutdown_releasesHeldDeployingLeader_evenIfHandlerNowSaysLeader() {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);
        when(mockSegmentingHandler.isEnabled()).thenReturn(true);
        when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        assertTrue(decider.isLeader(workerId));

        when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(LeaderLock.LEADER_HASH_KEY);
        decider.shutdown();

        assertTrue(getItem(LeaderLock.DEPLOYING_LEADER_HASH_KEY).isEmpty(), "DeployingLeader lock was not released");
        verify(mockSegmentingHandler, times(1)).getHashKeyForLeaderLock();
        verify(mockSegmentingHandler).setHeldLeaderKey(null);
    }

    /**
     * End to end with a real handler and version DAO: the lock holder writes its own version item, the lock item
     * carries no version, and after release the lock is deleted while the version item is left to expire.
     */
    @Test
    void versionItem_writtenByLockHolder_andKeptAfterRelease() throws Exception {
        final String owner = "versionOwner";
        final CoordinatorStateDAO sharedDao = newDao();
        final FleetSegmentingHandler handler =
                new FleetSegmentingHandler(segmentingConfig(owner), sharedDao, inlineExecutor(), () -> true);
        final DynamoDBLockBasedLeaderDecider decider = DynamoDBLockBasedLeaderDecider.create(
                sharedDao, owner, 100L, 10L, new NullMetricsFactory(), mockTableMigrationStateMachine, handler);
        decider.initialize();

        assertTrue(decider.isLeader(owner));

        final Map<String, AttributeValue> version = getItem(LeaderVersionState.LEADER_VERSION_HASH_KEY);
        assertEquals(
                owner, version.get(LeaderVersionState.OWNER_NAME_ATTRIBUTE_NAME).s());
        assertEquals(
                handler.getVersionHash(),
                version.get(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME).s());
        assertEquals("LEADER", version.get("entityType").s());
        assertNotNull(version.get(LeaderVersionState.HEARTBEAT_ATTRIBUTE_NAME).n());
        assertFalse(getLeaderItem().containsKey(FleetSegmentingHandler.VERSION_HASH_KEY));
        assertEquals(
                handler.getVersionHash(),
                handler.getLeaderVersionDAO()
                        .getVersionState(LeaderLock.LEADER_HASH_KEY)
                        .getVersionHash());

        decider.shutdown();

        assertTrue(getLeaderItem().isEmpty());
        assertFalse(getItem(LeaderVersionState.LEADER_VERSION_HASH_KEY).isEmpty());
    }

    /** A worker that doesn't hold the lock never writes the version item. */
    @Test
    void versionItem_notWrittenByNonLeader() {
        final String leaderId = getWorkerId(1);
        assertTrue(workerIdToLeaderDeciderMap.get(leaderId).isLeader(leaderId));

        final String other = "notLeader";
        final CoordinatorStateDAO sharedDao = newDao();
        final FleetSegmentingHandler handler =
                new FleetSegmentingHandler(segmentingConfig(other), sharedDao, inlineExecutor(), () -> true);
        final DynamoDBLockBasedLeaderDecider decider = DynamoDBLockBasedLeaderDecider.create(
                sharedDao, other, 100L, 10L, new NullMetricsFactory(), mockTableMigrationStateMachine, handler);
        decider.initialize();

        assertFalse(decider.isLeader(other));

        assertTrue(getItem(LeaderVersionState.LEADER_VERSION_HASH_KEY).isEmpty());
        decider.shutdown();
    }

    /**
     * The Leader holder crashed: its LeaderVersion (another version) stopped being heartbeated and nobody holds Leader.
     * A worker on the deploying version takes or keeps DeployingLeader instead of racing for Leader, and only moves to
     * Leader once every active worker reports its version.
     */
    @Test
    void leaderStoppedHeartbeating_deployingVersionWorkerStaysOnDeployingLeader_untilFleetIsOnItsVersion()
            throws Exception {
        final String workerId = "deployingVersionWorker";
        final CoordinatorStateDAO sharedDao = newDao();
        final FleetSegmentingHandler handler =
                new FleetSegmentingHandler(segmentingConfig(workerId), sharedDao, inlineExecutor(), () -> true);
        final DynamoDBLockBasedLeaderDecider decider = DynamoDBLockBasedLeaderDecider.create(
                sharedDao, workerId, 100L, 10L, new NullMetricsFactory(), mockTableMigrationStateMachine, handler);
        decider.initialize();
        // The crashed Leader's version: another version, last heartbeat 90s ago (past a worker's 60s staleness, well
        // within the leader version expiry).
        putLeaderVersion("crashedLeader", "otherVersion", System.currentTimeMillis() - 90_000L);

        assertTrue(decider.isLeader(workerId));
        assertEquals(
                workerId,
                getItem(LeaderLock.DEPLOYING_LEADER_HASH_KEY).get("ownerName").s());
        assertTrue(getLeaderItem().isEmpty(), "deploying-version worker took the Leader lock");
        assertEquals(
                "otherVersion",
                getItem(LeaderVersionState.LEADER_VERSION_HASH_KEY)
                        .get(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME)
                        .s());

        // Its assignment run sees every active worker on its version (the old-version worker is gone).
        final software.amazon.kinesis.worker.metricstats.WorkerMetricStats self =
                mock(software.amazon.kinesis.worker.metricstats.WorkerMetricStats.class);
        when(self.getWorkerId()).thenReturn(workerId);
        handler.setIsVersionEmittedByAllActiveWorkers(
                java.util.Collections.singletonList(self), java.util.Collections.singletonList(self));
        Thread.sleep(20); // past the 10ms heartbeat period, so the key is re-evaluated

        assertTrue(decider.isLeader(workerId));
        assertEquals(workerId, getLeaderItem().get("ownerName").s());
        assertTrue(getItem(LeaderLock.DEPLOYING_LEADER_HASH_KEY).isEmpty(), "DeployingLeader was not released");
        assertEquals(
                handler.getVersionHash(),
                getItem(LeaderVersionState.LEADER_VERSION_HASH_KEY)
                        .get(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME)
                        .s());
        decider.shutdown();
    }

    /** The crashed Leader was on this worker's version: it contends for Leader and takes over. */
    @Test
    void leaderStoppedHeartbeating_currentVersionWorkerTakesLeader() throws Exception {
        final String workerId = "currentVersionWorker";
        final CoordinatorStateDAO sharedDao = newDao();
        final FleetSegmentingHandler handler =
                new FleetSegmentingHandler(segmentingConfig(workerId), sharedDao, inlineExecutor(), () -> true);
        final DynamoDBLockBasedLeaderDecider decider = DynamoDBLockBasedLeaderDecider.create(
                sharedDao, workerId, 100L, 10L, new NullMetricsFactory(), mockTableMigrationStateMachine, handler);
        decider.initialize();
        putLeaderVersion("crashedLeader", handler.getVersionHash(), System.currentTimeMillis() - 90_000L);

        assertTrue(decider.isLeader(workerId));

        assertEquals(workerId, getLeaderItem().get("ownerName").s());
        assertEquals(
                workerId,
                getItem(LeaderVersionState.LEADER_VERSION_HASH_KEY)
                        .get(LeaderVersionState.OWNER_NAME_ATTRIBUTE_NAME)
                        .s());
        decider.shutdown();
    }

    private void putLeaderVersion(final String owner, final String versionHash, final long heartbeatMillis) {
        final Map<String, AttributeValue> item = new HashMap<>(
                new LeaderVersionState(LeaderVersionState.LEADER_VERSION_HASH_KEY, versionHash, owner, heartbeatMillis)
                        .serialize());
        item.put(
                DynamoDBLeaseSerializer.LEASE_KEY_KEY,
                AttributeValue.fromS(LeaderVersionState.LEADER_VERSION_HASH_KEY));
        dynamoDBSyncClient.putItem(PutItemRequest.builder()
                .tableName(TEST_LOCK_TABLE_NAME)
                .item(item)
                .build());
    }

    private static LeaseManagementConfig segmentingConfig(final String workerId) {
        final LeaseManagementConfig config = new LeaseManagementConfig(
                "dummyTableName",
                "dummyApplicationName",
                mock(software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient.class),
                mock(software.amazon.awssdk.services.kinesis.KinesisAsyncClient.class),
                workerId);
        config.enableRollingDeploymentSystem(true);
        return config;
    }

    /** An executor that runs submitted heartbeats inline and ignores the periodic schedule. */
    private static java.util.concurrent.ScheduledExecutorService inlineExecutor() {
        final java.util.concurrent.ScheduledExecutorService executor =
                mock(java.util.concurrent.ScheduledExecutorService.class);
        org.mockito.Mockito.doAnswer(invocation -> {
                    ((Runnable) invocation.getArgument(0)).run();
                    return null;
                })
                .when(executor)
                .execute(any(Runnable.class));
        return executor;
    }

    private Map<String, AttributeValue> getLeaderItem() {
        return getItem(LeaderLock.LEADER_HASH_KEY);
    }

    private Map<String, AttributeValue> getItem(final String key) {
        return dynamoDBSyncClient
                .getItem(GetItemRequest.builder()
                        .tableName(TEST_LOCK_TABLE_NAME)
                        .consistentRead(true)
                        .key(Collections.singletonMap(
                                DynamoDBLeaseSerializer.LEASE_KEY_KEY,
                                AttributeValue.builder().s(key).build()))
                        .build())
                .item();
    }

    @Test
    void isLeader_staleLeaderLock_assertLockTakenByAnotherWorker() throws InterruptedException {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);

        createRandomStaleLockEntry();

        // The First time we check the library starts in-memory counter for expiry tracking and does not get lock
        assertFalse(decider.isLeader(workerId), workerId + " got lock which is not expected as this is the first call");
        // lock lease Duration is 100ms sleep for 200ms to let the lock expire
        Thread.sleep(200);
        // another worker is able to take the lock now.
        assertTrue(decider.isLeader(workerId), workerId + " did not get the expired lock");
    }

    private void createRandomStaleLockEntry() {
        final PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(TEST_LOCK_TABLE_NAME)
                .item(ImmutableMap.of(
                        DynamoDBLeaseSerializer.LEASE_KEY_KEY,
                        AttributeValue.builder().s(LeaderLock.LEADER_HASH_KEY).build(),
                        "leaseDuration",
                        AttributeValue.builder().s("200").build(),
                        "ownerName",
                        AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                        "recordVersionNumber",
                        AttributeValue.builder().s(UUID.randomUUID().toString()).build()))
                .build();
        dynamoDBSyncClient.putItem(putItemRequest);
    }

    @Test
    void isLeader_doesNotReleaseExistingLockWithMatchingVersionHash() {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);

        // Worker acquires the lock first
        assertTrue(decider.isLeader(workerId));

        // Another worker with the same version hash should not release the lock
        final String otherWorkerId = getWorkerId(2);
        final DynamoDBLockBasedLeaderDecider otherDecider = workerIdToLeaderDeciderMap.get(otherWorkerId);
        assertFalse(otherDecider.isLeader(otherWorkerId));

        // Original worker should still be leader
        assertTrue(decider.isLeader(workerId));
    }

    @Test
    void isLeader_invokesHandleLeaderLockResult_whenLeaderIsTrue() throws Exception {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);

        assertTrue(decider.isLeader(workerId));

        verify(mockTableMigrationStateMachine).handleLeaderLockResult(true);
    }

    @Test
    void isLeader_invokesHandleLeaderLockResult_whenLeaderIsFalse() throws Exception {
        // Use a worker that won't get the lock (another worker holds it)
        final String leaderWorkerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider leaderDecider = workerIdToLeaderDeciderMap.get(leaderWorkerId);
        assertTrue(leaderDecider.isLeader(leaderWorkerId));

        final String nonLeaderWorkerId = getWorkerId(2);
        final DynamoDBLockBasedLeaderDecider nonLeaderDecider = workerIdToLeaderDeciderMap.get(nonLeaderWorkerId);
        assertFalse(nonLeaderDecider.isLeader(nonLeaderWorkerId));

        verify(mockTableMigrationStateMachine).handleLeaderLockResult(false);
    }

    @Test
    void isLeader_returnsFalse_whenHandleLeaderLockResultThrowsDependencyException() throws Exception {
        doThrow(new DependencyException("test", new RuntimeException()))
                .when(mockTableMigrationStateMachine)
                .handleLeaderLockResult(anyBoolean());

        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);

        assertFalse(decider.isLeader(workerId));
    }

    /**
     * Verifies that when handleLeaderLockResult throws InvalidStateException (signaling migration
     * to COMPLETE):
     * 1. isLeader returns false and the lock is released
     * 2. The lock client switches from the legacy table to the lease table
     *
     * Scenario:
     * 1. Start with PENDING status → DAO routes to legacy table lock client
     * 2. Worker acquires lock on legacy table, handleLeaderLockResult throws → returns false
     * 3. Status flips to COMPLETE → DAO routes to lease table lock client
     * 4. Next isLeader() acquires lock on lease table → returns true
     * 5. Verify lock exists on lease table with correct owner
     */
    @Test
    void isLeader_returnsFalseAndSwitchesLockClient_whenHandleLeaderLockResultThrowsInvalidStateException()
            throws Exception {
        // Create a separate legacy coordinator state table with "key" as partition key
        final String legacyTableName = "LegacyCoordinatorStateTable";
        dynamoDBAsyncClient
                .createTable(CreateTableRequest.builder()
                        .tableName(legacyTableName)
                        .keySchema(KeySchemaElement.builder()
                                .attributeName("key")
                                .keyType(KeyType.HASH)
                                .build())
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("key")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .build())
                .join();

        // Set up TableMigrationStatusProvider starting at PENDING
        final TableMigrationStatusProvider mockProvider = mock(TableMigrationStatusProvider.class);
        when(mockProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING);

        // Create DAO with PENDING status (both legacy and lease table lock clients will be created)
        final CoordinatorConfig c = new CoordinatorConfig("TestMigrationApp");
        c.coordinatorStateTableConfig().tableName(legacyTableName);
        final CoordinatorStateDAO dao = new CoordinatorStateDAO(
                dynamoDBAsyncClient,
                c.coordinatorStateTableConfig(),
                TEST_LOCK_TABLE_NAME,
                mockProvider,
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT);
        dao.initializeDelegates();
        dao.initialize();

        // Create a mock table migration state machine that throws on first call (simulating COMPLETE flip)
        final TableMigrationStateMachine mockMigrationSM = mock(TableMigrationStateMachine.class);
        doThrow(new InvalidStateException("migration complete - switch lock client"))
                .when(mockMigrationSM)
                .handleLeaderLockResult(true);

        // Create the leader decider
        final String workerId = "migrationTestWorker";
        final DynamoDBLockBasedLeaderDecider decider = DynamoDBLockBasedLeaderDecider.create(
                dao, workerId, 100L, 10L, new NullMetricsFactory(), mockMigrationSM, mockSegmentingHandler);
        decider.initialize();

        // Step 1: isLeader() acquires lock on legacy table, but handleLeaderLockResult throws
        // → lock is released, returns false
        assertFalse(
                decider.isLeader(workerId), "Expected false: handleLeaderLockResult threw, lock should be released");

        // Step 2: Flip migration status to COMPLETE
        when(mockProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        // Reset the mock to allow handleLeaderLockResult to succeed on next call
        org.mockito.Mockito.reset(mockMigrationSM);

        // Wait for heartbeat period to expire so the cached false result is not returned
        Thread.sleep(15);

        // Step 3: Next isLeader() should use lease table lock client and succeed
        assertTrue(
                decider.isLeader(workerId),
                "Expected true: after status flip to COMPLETE, lock should be acquired on lease table");

        // Verify the lock is held on the LEASE table (not the legacy table)
        final GetItemResponse leaseTableLockItem = dynamoDBSyncClient.getItem(GetItemRequest.builder()
                .tableName(TEST_LOCK_TABLE_NAME)
                .key(Collections.singletonMap(
                        DynamoDBLeaseSerializer.LEASE_KEY_KEY,
                        AttributeValue.builder().s(LeaderLock.LEADER_HASH_KEY).build()))
                .build());
        assertTrue(leaseTableLockItem.hasItem(), "Lock should exist in lease table");
        assertEquals(
                workerId,
                leaseTableLockItem.item().get("ownerName").s(),
                "Lock in lease table should be owned by our worker");

        decider.shutdown();
    }
}
