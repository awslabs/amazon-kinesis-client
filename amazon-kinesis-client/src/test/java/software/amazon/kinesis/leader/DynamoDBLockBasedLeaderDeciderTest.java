package software.amazon.kinesis.leader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
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
import static org.mockito.Mockito.mock;
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

    @BeforeEach
    void setup() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final CoordinatorConfig c = new CoordinatorConfig("TestApplication");
        c.coordinatorStateTableConfig().tableName(TEST_LOCK_TABLE_NAME);
        final TableMigrationStatusProvider provider = mock(TableMigrationStatusProvider.class);
        when(provider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        leaseRefresher.createLeaseTableIfNotExists();
        final CoordinatorStateDAO dao = new CoordinatorStateDAO(
                dynamoDBAsyncClient, c.coordinatorStateTableConfig(), TEST_LOCK_TABLE_NAME, provider);
        dao.initialize();
        IntStream.range(0, 10).sequential().forEach(index -> {
            final String workerId = getWorkerId(index);
            workerIdToLeaderDeciderMap.put(
                    workerId,
                    DynamoDBLockBasedLeaderDecider.create(dao, workerId, 100L, 10L, new NullMetricsFactory()));
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
}
