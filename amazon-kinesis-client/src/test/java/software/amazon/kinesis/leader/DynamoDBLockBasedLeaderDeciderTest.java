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
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.LeaseAssignmentStrategy;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.segmenting.FleetSegmentingHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DynamoDBLockBasedLeaderDeciderTest {

    private static final String TEST_LOCK_TABLE_NAME = "IAmTestLockTable";

    private final AmazonDynamoDBLocal dynamoDBEmbedded = DynamoDBEmbedded.create();
    private final DynamoDbAsyncClient dynamoDBAsyncClient = dynamoDBEmbedded.dynamoDbAsyncClient();
    private final DynamoDbClient dynamoDBSyncClient = dynamoDBEmbedded.dynamoDbClient();
    private final Map<String, DynamoDBLockBasedLeaderDecider> workerIdToLeaderDeciderMap = new HashMap<>();

    private FleetSegmentingHandler mockSegmentingHandler;

    @BeforeEach
    void setup() throws DependencyException {
        final CoordinatorConfig c = new CoordinatorConfig("TestApplication");
        c.coordinatorStateTableConfig().tableName(TEST_LOCK_TABLE_NAME);
        final CoordinatorStateDAO dao = new CoordinatorStateDAO(dynamoDBAsyncClient, c.coordinatorStateTableConfig());
        dao.initialize();
        mockSegmentingHandler = mock(FleetSegmentingHandler.class);
        Mockito.when(mockSegmentingHandler.getVersionHashKey()).thenReturn("versionHash");
        Mockito.when(mockSegmentingHandler.getVersionHash())
                .thenReturn(String.valueOf(
                        LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE.name().hashCode()));
        Mockito.when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(CoordinatorState.LEADER_HASH_KEY);
        IntStream.range(0, 10).sequential().forEach(index -> {
            final String workerId = getWorkerId(index);
            workerIdToLeaderDeciderMap.put(
                    workerId,
                    DynamoDBLockBasedLeaderDecider.create(
                            dao, workerId, 100L, 10L, new NullMetricsFactory(), mockSegmentingHandler));
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
                        CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME,
                        AttributeValue.builder()
                                .s(CoordinatorState.LEADER_HASH_KEY)
                                .build()))
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
                        CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME,
                        AttributeValue.builder()
                                .s(CoordinatorState.LEADER_HASH_KEY)
                                .build(),
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
    void isLeader_releasesDeployingLeaderLockWithDifferentVersionHash() {
        // Worker 1 acquires the deploying leader lock
        Mockito.when(mockSegmentingHandler.getHashKeyForLeaderLock())
                .thenReturn(CoordinatorState.DEPLOYING_LEADER_HASH_KEY);
        final String workerId1 = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider1 = workerIdToLeaderDeciderMap.get(workerId1);
        assertTrue(decider1.isLeader(workerId1));

        // Change version hash so the lock no longer matches, and target the leader key
        Mockito.when(mockSegmentingHandler.getVersionHash()).thenReturn("newVersionHash");
        Mockito.when(mockSegmentingHandler.getHashKeyForLeaderLock()).thenReturn(CoordinatorState.LEADER_HASH_KEY);

        // Worker 1 calls isLeader — should release the deploying lock (version mismatch)
        decider1.isLeader(workerId1);

        // Worker 2 should be able to acquire the deploying leader lock since worker 1 released it
        Mockito.when(mockSegmentingHandler.getHashKeyForLeaderLock())
                .thenReturn(CoordinatorState.DEPLOYING_LEADER_HASH_KEY);
        Mockito.when(mockSegmentingHandler.getVersionHash()).thenReturn("worker2Version");
        final String workerId2 = getWorkerId(2);
        final DynamoDBLockBasedLeaderDecider decider2 = workerIdToLeaderDeciderMap.get(workerId2);
        assertTrue(decider2.isLeader(workerId2), "Worker 2 should acquire the released deploying leader lock");
    }
}
