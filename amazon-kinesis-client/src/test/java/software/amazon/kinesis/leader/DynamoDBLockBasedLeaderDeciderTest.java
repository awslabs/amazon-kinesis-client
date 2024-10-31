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
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamoDBLockBasedLeaderDeciderTest {

    private static final String TEST_LOCK_TABLE_NAME = "IAmTestLockTable";

    private final AmazonDynamoDBLocal dynamoDBEmbedded = DynamoDBEmbedded.create();
    private final DynamoDbAsyncClient dynamoDBAsyncClient = dynamoDBEmbedded.dynamoDbAsyncClient();
    private final DynamoDbClient dynamoDBSyncClient = dynamoDBEmbedded.dynamoDbClient();
    private final Map<String, DynamoDBLockBasedLeaderDecider> workerIdToLeaderDeciderMap = new HashMap<>();

    @BeforeEach
    void setup() throws DependencyException {
        final CoordinatorConfig c = new CoordinatorConfig("TestApplication");
        c.coordinatorStateConfig().tableName(TEST_LOCK_TABLE_NAME);
        final CoordinatorStateDAO dao = new CoordinatorStateDAO(dynamoDBAsyncClient, c.coordinatorStateConfig());
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

    @Test
    void isAnyLeaderElected_sanity() throws InterruptedException {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);

        assertFalse(decider.isAnyLeaderElected(), "isAnyLeaderElected returns true when no leader lock is present");

        // perform leaderElection
        decider.isLeader(workerId);
        Thread.sleep(120);

        assertTrue(decider.isAnyLeaderElected(), "isAnyLeaderElected returns false when leader lock is present");

        Thread.sleep(120);
        // heartbeat is happening on the leader validate that on different identifying different RevisionNumber,
        // lock is considered ACTIVE
        assertTrue(decider.isAnyLeaderElected(), "isAnyLeaderElected returns false when leader lock is present");
    }

    @Test
    void isAnyLeaderElected_staleLock_validateExpectedBehavior() throws InterruptedException {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);

        createRandomStaleLockEntry();

        assertTrue(decider.isAnyLeaderElected(), "isAnyLeaderElected returns false when leader lock is present");

        // sleep for more than lease duration
        Thread.sleep(205);

        // lock has become stale as it passed lease duration without any heartbeat
        assertFalse(decider.isAnyLeaderElected(), "isAnyLeaderElected returns true when leader lock is stale");
    }

    @Test
    void isAnyLeaderElected_withoutTable_assertFalse() {
        final String workerId = getWorkerId(1);
        final DynamoDBLockBasedLeaderDecider decider = workerIdToLeaderDeciderMap.get(workerId);

        dynamoDBSyncClient.deleteTable(
                DeleteTableRequest.builder().tableName(TEST_LOCK_TABLE_NAME).build());
        assertFalse(decider.isAnyLeaderElected(), "isAnyLeaderElected returns true when table don't exists");
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
}
