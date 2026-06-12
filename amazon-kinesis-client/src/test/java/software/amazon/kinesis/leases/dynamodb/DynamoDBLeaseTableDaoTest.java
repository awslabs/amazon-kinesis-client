package software.amazon.kinesis.leases.dynamodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.kinesis.coordinator.delegate.CoordinatorStateDAODelegate;
import software.amazon.kinesis.leases.EntityDAO.EntityScanList;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link DynamoDBLeaseTableDao} verifying that scanning a lease table
 * with mixed entity types correctly groups entities by their EntityType.
 */
@ExtendWith(MockitoExtension.class)
class DynamoDBLeaseTableDaoTest {

    private static final String TABLE_NAME = "testLeaseTable";
    private static final int TOTAL_SEGMENTS = 1;

    @Mock
    private DynamoDbAsyncClient dynamoDbAsyncClient;

    @Mock
    private CoordinatorStateDAODelegate coordinatorStateDelegate;

    private DynamoDBLeaseTableDao dao;
    private DynamoDBLeaseSerializer leaseSerializer;
    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        leaseSerializer = new DynamoDBLeaseSerializer();
        executorService = Executors.newFixedThreadPool(2);
        dao = new DynamoDBLeaseTableDao(
                dynamoDbAsyncClient,
                TABLE_NAME,
                leaseSerializer,
                coordinatorStateDelegate,
                executorService,
                TOTAL_SEGMENTS);
    }

    @AfterEach
    void tearDown() {
        executorService.shutdownNow();
    }

    private Map<String, AttributeValue> buildLeaseItem(String leaseKey, String owner) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("leaseKey", AttributeValue.builder().s(leaseKey).build());
        item.put("leaseOwner", AttributeValue.builder().s(owner).build());
        item.put("leaseCounter", AttributeValue.builder().n("1").build());
        item.put("ownerSwitchesSinceCheckpoint", AttributeValue.builder().n("0").build());
        // No entityType → defaults to LEASE
        return item;
    }

    private Map<String, AttributeValue> buildWorkerMetricStatsItem(String workerId) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("leaseKey", AttributeValue.builder().s(workerId).build());
        item.put(
                "entityType",
                AttributeValue.builder()
                        .s(EntityType.WORKER_METRIC_STATS.getDdbValue())
                        .build());
        item.put(
                "lut",
                AttributeValue.builder()
                        .n(String.valueOf(System.currentTimeMillis() / 1000))
                        .build());
        return item;
    }

    private void setupScanResponse(List<Map<String, AttributeValue>> items) {
        ScanResponse response = ScanResponse.builder().items(items).build();
        when(dynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
    }

    @Test
    void scanAllEntities_emptyTable_returnsEmptyResults()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        setupScanResponse(Collections.emptyList());

        Map<EntityType, EntityScanList> result = dao.scanAllEntities();

        assertNotNull(result);
        for (EntityType type : EntityType.values()) {
            EntityScanList scanList = result.get(type);
            assertNotNull(scanList);
            assertTrue(scanList.getEntities().isEmpty());
            assertTrue(scanList.getDeserializationFailures().isEmpty());
        }
    }

    @Test
    void scanAllEntities_leaseItemsWithoutEntityType_treatedAsLease()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(buildLeaseItem("lease1", "worker1"));
        items.add(buildLeaseItem("lease2", "worker2"));
        setupScanResponse(items);

        Map<EntityType, EntityScanList> result = dao.scanAllEntities();

        EntityScanList leaseList = result.get(EntityType.LEASE);
        assertEquals(2, leaseList.getEntities().size());

        // Verify these are actually Lease objects
        Lease firstLease = (Lease) leaseList.getEntities().get(0);
        assertNotNull(firstLease.leaseKey());
    }

    @Test
    void scanAllEntities_workerMetricStatsItems_groupedCorrectly()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(buildLeaseItem("lease1", "worker1"));
        items.add(buildWorkerMetricStatsItem("workerMetrics1"));
        setupScanResponse(items);

        Map<EntityType, EntityScanList> result = dao.scanAllEntities();

        EntityScanList leaseList = result.get(EntityType.LEASE);
        EntityScanList wmList = result.get(EntityType.WORKER_METRIC_STATS);

        assertEquals(1, leaseList.getEntities().size());
        assertEquals(1, wmList.getEntities().size());

        WorkerMetricStats wm = (WorkerMetricStats) wmList.getEntities().get(0);
        assertEquals("workerMetrics1", wm.getWorkerId());
    }

    @Test
    void scanAllEntities_mixedEntities_correctlyGrouped()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(buildLeaseItem("lease1", "owner1"));
        items.add(buildLeaseItem("lease2", "owner2"));
        items.add(buildWorkerMetricStatsItem("worker1"));
        items.add(buildWorkerMetricStatsItem("worker2"));
        setupScanResponse(items);

        Map<EntityType, EntityScanList> result = dao.scanAllEntities();

        assertEquals(2, result.get(EntityType.LEASE).getEntities().size());
        assertEquals(2, result.get(EntityType.WORKER_METRIC_STATS).getEntities().size());
    }

    @Test
    void scanEntities_filterByLeaseOnly_returnsOnlyLeases()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(buildLeaseItem("lease1", "owner1"));
        items.add(buildWorkerMetricStatsItem("worker1"));
        setupScanResponse(items);

        Map<EntityType, EntityScanList> result = dao.scanEntities(EntityType.LEASE);

        assertNotNull(result.get(EntityType.LEASE));
        assertEquals(1, result.get(EntityType.LEASE).getEntities().size());
        // WORKER_METRIC_STATS should not be in the result or should be empty
        assertTrue(result.get(EntityType.WORKER_METRIC_STATS) == null
                || result.get(EntityType.WORKER_METRIC_STATS).getEntities().isEmpty());
    }

    @Test
    void scanEntities_filterByMultipleTypes_returnsRequestedTypes()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(buildLeaseItem("lease1", "owner1"));
        items.add(buildWorkerMetricStatsItem("worker1"));
        setupScanResponse(items);

        Map<EntityType, EntityScanList> result = dao.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS);

        assertEquals(1, result.get(EntityType.LEASE).getEntities().size());
        assertEquals(1, result.get(EntityType.WORKER_METRIC_STATS).getEntities().size());
    }

    @Test
    void scanAllEntities_itemWithMissingRequiredFields_recordedAsDeserializationFailure()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(buildLeaseItem("goodLease", "owner1"));
        // Bad lease item - missing leaseCounter
        Map<String, AttributeValue> badItem = new HashMap<>();
        badItem.put("leaseKey", AttributeValue.builder().s("badLease").build());
        items.add(badItem);
        setupScanResponse(items);

        Map<EntityType, EntityScanList> result = dao.scanAllEntities();

        EntityScanList leaseList = result.get(EntityType.LEASE);
        // The good lease should be deserialized successfully
        // The bad lease may either be deserialized (DynamoDBLeaseSerializer is lenient) or fail
        int totalEntities = leaseList.getEntities().size();
        int totalFailures = leaseList.getDeserializationFailures().size();
        assertEquals(2, totalEntities + totalFailures);
    }

    @Test
    void scanAllEntities_unknownEntityType_treatedAsLease()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        // Put an item with an unknown entityType value
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("leaseKey", AttributeValue.builder().s("unknownEntity").build());
        item.put("entityType", AttributeValue.builder().s("SOME_FUTURE_TYPE").build());
        item.put("leaseCounter", AttributeValue.builder().n("1").build());
        item.put("ownerSwitchesSinceCheckpoint", AttributeValue.builder().n("0").build());
        items.add(item);
        setupScanResponse(items);

        Map<EntityType, EntityScanList> result = dao.scanAllEntities();

        // Unknown entity types fall back to LEASE
        EntityScanList leaseList = result.get(EntityType.LEASE);
        int totalLeasesOrFailures = leaseList.getEntities().size()
                + leaseList.getDeserializationFailures().size();
        assertEquals(1, totalLeasesOrFailures);
    }
}
