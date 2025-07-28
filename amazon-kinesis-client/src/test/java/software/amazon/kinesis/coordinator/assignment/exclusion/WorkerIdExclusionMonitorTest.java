package software.amazon.kinesis.coordinator.assignment.exclusion;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.leader.DynamoDBLockBasedLeaderDecider;
import software.amazon.kinesis.leases.DynamoUtils;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.kinesis.coordinator.CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME;

class WorkerIdExclusionMonitorTest {

    private static final String TEST_COORDINATOR_STATE_TABLE_NAME = "IAmTestCoordinatorStateTable";

    private final AmazonDynamoDBLocal dynamoDBEmbedded = DynamoDBEmbedded.create();
    private final DynamoDbAsyncClient dynamoDBAsyncClient = dynamoDBEmbedded.dynamoDbAsyncClient();
    private final Map<String, DynamoDBLockBasedLeaderDecider> leaderDeciders = new HashMap<>();

    private CoordinatorStateDAO dao;
    private WorkerIdExclusionMonitor monitor;

    @BeforeEach
    void setup() throws DependencyException {
        final CoordinatorConfig c = new CoordinatorConfig("TestApplication");
        c.coordinatorStateTableConfig().tableName(TEST_COORDINATOR_STATE_TABLE_NAME);

        this.dao = new CoordinatorStateDAO(dynamoDBAsyncClient, c.coordinatorStateTableConfig());
        this.dao.initialize();

        for (int i = 0; i < 3; i++) {
            String workerId = getWorkerId(i);
            DynamoDBLockBasedLeaderDecider leaderDecider =
                    DynamoDBLockBasedLeaderDecider.create(this.dao, workerId, 100L, 10L, new NullMetricsFactory());
            leaderDecider.initialize();
            leaderDeciders.put(workerId, leaderDecider);
        }

        WorkerIdExclusionMonitor.create(this.dao, Executors.newScheduledThreadPool(1));
        this.monitor = WorkerIdExclusionMonitor.getInstance();
        // shut down the monitor as we want to manually invoke run() and not wait the MONITOR_INTERVAL_MILLIS
        this.monitor.shutdown();
    }

    @AfterEach
    void reset() {
        try {
            Field instanceField = WorkerIdExclusionMonitor.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testEmptyState() {
        this.monitor.run();

        assertEquals(null, this.monitor.getActivePattern());
        assertEquals(false, this.monitor.isOnlyExcludingLeadership());
        assertEquals(false, this.monitor.isNewState());
    }

    @Test
    void testNewState() throws Exception {
        this.monitor.run();
        assertEquals(false, this.monitor.isNewState());

        WorkerIdExclusionState defaultState = new WorkerIdExclusionState();
        this.dao.createCoordinatorStateIfNotExists(defaultState);
        this.verifyNewState();

        WorkerIdExclusionState newState = new WorkerIdExclusionState(getWorkerId(0), Long.MAX_VALUE, false);
        this.dao.updateCoordinatorStateWithExpectation(newState, getNewExpectedAVs());
        this.verifyNewState();

        this.deleteWorkerIdExclusionItem();
        this.verifyNewState();

        this.dao.createCoordinatorStateIfNotExists(defaultState);
        this.verifyNewState();
    }

    private void verifyNewState() {
        this.monitor.run();
        assertEquals(true, this.monitor.isNewState());

        for (int i = 0; i < 2; i++) {
            this.monitor.run();
            assertEquals(false, this.monitor.isNewState());
        }
    }

    @Test
    void testLeaderExcluded() throws Exception {
        String workerId0 = getWorkerId(0);
        String workerId1 = getWorkerId(1);
        LeaderDecider leaderDecider0 = this.leaderDeciders.get(workerId0);
        LeaderDecider leaderDecider1 = this.leaderDeciders.get(workerId1);

        assertEquals(true, leaderDecider0.isLeader(workerId0));
        assertEquals(false, leaderDecider1.isLeader(workerId1));

        WorkerIdExclusionState state = new WorkerIdExclusionState(workerId0, Long.MAX_VALUE, false);
        this.dao.createCoordinatorStateIfNotExists(state);

        this.monitor.run();
        assertEquals(false, leaderDecider0.isLeader(workerId0));
        Thread.sleep(10L);
        assertEquals(true, leaderDecider1.isLeader(workerId1));

        WorkerIdExclusionState newState = new WorkerIdExclusionState(workerId1, Long.MAX_VALUE, false);
        this.dao.updateCoordinatorStateWithExpectation(newState, getNewExpectedAVs());

        this.monitor.run();
        assertEquals(false, leaderDecider1.isLeader(workerId1));
        Thread.sleep(10L);
        assertEquals(true, leaderDecider0.isLeader(workerId0));
    }

    @Test
    void testActivePatternAttribute() throws Exception {
        String workerId0 = getWorkerId(0);
        String workerId1 = getWorkerId(1);

        WorkerIdExclusionState state = new WorkerIdExclusionState();
        this.dao.createCoordinatorStateIfNotExists(state);

        this.monitor.run();
        assertEquals(false, this.monitor.isExcluded(workerId0));
        assertEquals(false, this.monitor.isExcluded(workerId1));

        WorkerIdExclusionState newState = new WorkerIdExclusionState(workerId0, Long.MAX_VALUE, false);
        this.dao.updateCoordinatorStateWithExpectation(newState, getNewExpectedAVs());

        this.monitor.run();
        assertEquals(true, this.monitor.isExcluded(workerId0));
        assertEquals(false, this.monitor.isExcluded(workerId1));
    }

    @Test
    void testExpirationAttribute() throws Exception {
        String workerId0 = getWorkerId(0);

        WorkerIdExclusionState state = new WorkerIdExclusionState(workerId0, Long.MAX_VALUE, false);
        this.dao.createCoordinatorStateIfNotExists(state);

        this.monitor.run();
        assertEquals(true, this.monitor.isExcluded(workerId0));

        WorkerIdExclusionState newState = new WorkerIdExclusionState(workerId0, 0L, false);
        this.dao.updateCoordinatorStateWithExpectation(newState, getNewExpectedAVs());

        this.monitor.run();
        assertEquals(false, this.monitor.isExcluded(workerId0));
    }

    @Test
    void testExcludingLeadershipAttribute() throws Exception {
        String workerId0 = getWorkerId(0);

        WorkerIdExclusionState falseState = new WorkerIdExclusionState(workerId0, Long.MAX_VALUE, false);
        this.dao.createCoordinatorStateIfNotExists(falseState);

        this.monitor.run();
        assertEquals(true, this.monitor.isExcluded(workerId0));
        assertEquals(true, this.monitor.isExcludedFromLeadership(workerId0));

        WorkerIdExclusionState trueState = new WorkerIdExclusionState(workerId0, Long.MAX_VALUE, true);
        this.dao.updateCoordinatorStateWithExpectation(trueState, getNewExpectedAVs());

        this.monitor.run();
        assertEquals(false, this.monitor.isExcluded(workerId0));
        assertEquals(true, this.monitor.isExcludedFromLeadership(workerId0));
    }

    private void deleteWorkerIdExclusionItem() throws Exception {
        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
                .tableName(TEST_COORDINATOR_STATE_TABLE_NAME)
                .key(Collections.singletonMap(
                        COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME,
                        DynamoUtils.createAttributeValue(WorkerIdExclusionState.WORKER_ID_EXCLUSION_HASH_KEY)))
                .build();

        DeleteItemResponse response =
                FutureUtils.unwrappingFuture(() -> this.dynamoDBAsyncClient.deleteItem(deleteItemRequest));
    }

    private static Map<String, ExpectedAttributeValue> getNewExpectedAVs() {
        Map<String, ExpectedAttributeValue> expectedAVs = new HashMap<String, ExpectedAttributeValue>();

        expectedAVs.put(
                COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME,
                ExpectedAttributeValue.builder()
                        .value(AttributeValue.fromS(WorkerIdExclusionState.WORKER_ID_EXCLUSION_HASH_KEY))
                        .build());

        return expectedAVs;
    }

    private static String getWorkerId(int index) {
        return "worker" + index;
    }
}
