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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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

    @BeforeEach
    void setup() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        leaseRefresher.createLeaseTableIfNotExists();
        mockTableMigrationStateMachine = mock(TableMigrationStateMachine.class);
        IntStream.range(0, 10).sequential().forEach(index -> {
            final String workerId = getWorkerId(index);
            final CoordinatorConfig c = new CoordinatorConfig("TestApplication");
            c.coordinatorStateTableConfig().tableName(TEST_LOCK_TABLE_NAME);
            final TableMigrationStatusProvider provider = mock(TableMigrationStatusProvider.class);
            when(provider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
            final CoordinatorStateDAO dao = new CoordinatorStateDAO(
                    dynamoDBAsyncClient, c.coordinatorStateTableConfig(), TEST_LOCK_TABLE_NAME, provider);
            try {
                dao.initialize();
            } catch (final InvalidStateException e) {
                throw new RuntimeException(e);
            }
            workerIdToLeaderDeciderMap.put(
                    workerId,
                    DynamoDBLockBasedLeaderDecider.create(
                            dao, workerId, 100L, 10L, new NullMetricsFactory(), mockTableMigrationStateMachine));
        });

        workerIdToLeaderDeciderMap.values().forEach(DynamoDBLockBasedLeaderDecider::initialize);
    }

    private static String getWorkerId(final int index) {
        return "worker" + index;
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
        // assert that after shutdown the lockItem is no longer present.
        assertFalse(getItemResult.hasItem());

        // After shutdown, assert that leaderDecider returns false.
        assertFalse(decider.isLeader(workerId), "LeaderDecider did not return false after shutdown.");
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
                dynamoDBAsyncClient, c.coordinatorStateTableConfig(), TEST_LOCK_TABLE_NAME, mockProvider);
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
                dao, workerId, 100L, 10L, new NullMetricsFactory(), mockMigrationSM);
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
