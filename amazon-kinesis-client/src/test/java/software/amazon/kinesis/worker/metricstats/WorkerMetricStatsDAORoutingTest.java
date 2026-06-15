/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.worker.metricstats;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test for WorkerMetricStatsDAO routing using in-memory DynamoDB.
 * Verifies reads/writes go to the correct table based on TableMigrationStatus.
 */
class WorkerMetricStatsDAORoutingTest {

    private static final String LEGACY_TABLE = "worker-metrics-legacy";
    private static final String LEASE_TABLE = "worker-metrics-lease";

    private static AmazonDynamoDBLocal embeddedDdb;
    private static DynamoDbAsyncClient ddbClient;

    private TableMigrationStatusProvider statusProvider;

    @BeforeAll
    static void startDdb() throws Exception {
        embeddedDdb = DynamoDBEmbedded.create();
        ddbClient = embeddedDdb.dynamoDbAsyncClient();

        // Legacy worker metrics table (PK: wid)
        ddbClient
                .createTable(CreateTableRequest.builder()
                        .tableName(LEGACY_TABLE)
                        .keySchema(KeySchemaElement.builder()
                                .attributeName("wid")
                                .keyType(KeyType.HASH)
                                .build())
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("wid")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build())
                        .build())
                .get();

        // Lease table (PK: leaseKey)
        ddbClient
                .createTable(CreateTableRequest.builder()
                        .tableName(LEASE_TABLE)
                        .keySchema(KeySchemaElement.builder()
                                .attributeName("leaseKey")
                                .keyType(KeyType.HASH)
                                .build())
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("leaseKey")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build())
                        .build())
                .get();
    }

    @AfterAll
    static void stopDdb() {
        if (embeddedDdb != null) {
            embeddedDdb.shutdown();
        }
    }

    @BeforeEach
    void setUp() {
        statusProvider = mock(TableMigrationStatusProvider.class);
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_UNKNOWN);
    }

    private WorkerMetricStatsDAO createDao() {
        WorkerMetricsTableConfig workerMetricsConfig = new WorkerMetricsTableConfig(null);
        workerMetricsConfig.tableName(LEGACY_TABLE);

        return new WorkerMetricStatsDAO(ddbClient, workerMetricsConfig, LEASE_TABLE, 10000L, statusProvider);
    }

    // --- Initialization ---

    @Test
    void initialize_whenStatusUnknown_throws() {
        WorkerMetricStatsDAO dao = createDao();
        assertThrows(DependencyException.class, dao::initialize);
    }

    @Test
    void updateMetrics_beforeInit_throws() {
        WorkerMetricStatsDAO dao = createDao();
        WorkerMetricStats stats = new WorkerMetricStats();
        stats.setWorkerId("w1");
        assertThrows(InvalidStateException.class, () -> dao.updateMetrics(stats));
    }

    @Test
    void getAllWorkerMetricStats_beforeInit_throws() {
        WorkerMetricStatsDAO dao = createDao();
        assertThrows(InvalidStateException.class, dao::getAllWorkerMetricStats);
    }

    // --- Write routing ---

    @Test
    void updateMetrics_whenInit_writesToLegacyTable()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        WorkerMetricStats stats = new WorkerMetricStats();
        stats.setWorkerId("worker-legacy");
        stats.setLastUpdateTime(Instant.now().getEpochSecond());
        stats.setMetricStats(ImmutableMap.of("C", ImmutableList.of(50.0)));
        stats.setOperatingRange(ImmutableMap.of("C", ImmutableList.of(80L)));
        dao.updateMetrics(stats);

        // Verify written to legacy table
        Map<String, AttributeValue> item = getItem(LEGACY_TABLE, "wid", "worker-legacy");
        assertFalse(item.isEmpty());
        assertEquals("worker-legacy", item.get("wid").s());
    }

    @Test
    void updateMetrics_whenComplete_writesToLeaseTable()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(false);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        WorkerMetricStats stats = new WorkerMetricStats();
        stats.setWorkerId("worker-lease");
        stats.setLastUpdateTime(Instant.now().getEpochSecond());
        stats.setMetricStats(ImmutableMap.of("C", ImmutableList.of(60.0)));
        stats.setOperatingRange(ImmutableMap.of("C", ImmutableList.of(80L)));
        dao.updateMetrics(stats);

        // Verify written to lease table
        Map<String, AttributeValue> item = getItem(LEASE_TABLE, "leaseKey", "worker-lease");
        assertFalse(item.isEmpty());
    }

    @Test
    void updateMetrics_whenPending_writesToLeaseTable()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        WorkerMetricStats stats = new WorkerMetricStats();
        stats.setWorkerId("worker-pending");
        stats.setLastUpdateTime(Instant.now().getEpochSecond());
        stats.setMetricStats(ImmutableMap.of("C", ImmutableList.of(70.0)));
        stats.setOperatingRange(ImmutableMap.of("C", ImmutableList.of(80L)));
        dao.updateMetrics(stats);

        Map<String, AttributeValue> item = getItem(LEASE_TABLE, "leaseKey", "worker-pending");
        assertFalse(item.isEmpty());
    }

    @Test
    void updateMetrics_whenDeployed_writesToLegacyTable()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        WorkerMetricStats stats = new WorkerMetricStats();
        stats.setWorkerId("worker-deployed");
        stats.setLastUpdateTime(Instant.now().getEpochSecond());
        stats.setMetricStats(ImmutableMap.of("C", ImmutableList.of(40.0)));
        stats.setOperatingRange(ImmutableMap.of("C", ImmutableList.of(80L)));
        dao.updateMetrics(stats);

        Map<String, AttributeValue> item = getItem(LEGACY_TABLE, "wid", "worker-deployed");
        assertFalse(item.isEmpty());
    }

    // --- Read routing ---

    @Test
    void getAllWorkerMetricStats_whenComplete_readsOnlyFromLeaseTable() throws Exception {
        // Seed legacy table
        putWorkerMetricItem(LEGACY_TABLE, "wid", "legacy-only-worker");
        // Seed lease table
        putWorkerMetricItem(LEASE_TABLE, "leaseKey", "lease-worker");

        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(false);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        List<WorkerMetricStats> results = dao.getAllWorkerMetricStats();

        // Should only see lease table entries
        assertTrue(results.stream().anyMatch(s -> "lease-worker".equals(s.getWorkerId())));
        assertFalse(results.stream().anyMatch(s -> "legacy-only-worker".equals(s.getWorkerId())));
    }

    @Test
    void getAllWorkerMetricStats_whenInit_readsBothTables() throws Exception {
        // Seed legacy table
        putWorkerMetricItem(LEGACY_TABLE, "wid", "legacy-worker-both");
        // Seed lease table
        putWorkerMetricItem(LEASE_TABLE, "leaseKey", "lease-worker-both");

        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        List<WorkerMetricStats> results = dao.getAllWorkerMetricStats();

        // Should see entries from both tables
        assertTrue(results.stream().anyMatch(s -> "legacy-worker-both".equals(s.getWorkerId())));
        assertTrue(results.stream().anyMatch(s -> "lease-worker-both".equals(s.getWorkerId())));
    }

    // --- Delete routing ---

    @Test
    void deleteMetrics_whenInit_deletesFromLegacyTable() throws Exception {
        putWorkerMetricItem(LEGACY_TABLE, "wid", "delete-me");

        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        WorkerMetricStats stats = new WorkerMetricStats();
        stats.setWorkerId("delete-me");
        stats.setLastUpdateTime(Instant.now().getEpochSecond());
        boolean deleted = dao.deleteMetrics(stats);

        assertTrue(deleted);
        Map<String, AttributeValue> item = getItem(LEGACY_TABLE, "wid", "delete-me");
        assertTrue(item == null || item.isEmpty());
    }

    // --- Scan filtering ---

    @Test
    void getAllWorkerMetricStats_leaseTable_filtersOutNonWorkerMetricEntities() throws Exception {
        // Seed lease table with a WMS entry
        putWorkerMetricItem(LEASE_TABLE, "leaseKey", "wms-entry");
        // Seed lease table with a non-WMS entry (e.g. a lease or coordinator state)
        Map<String, AttributeValue> nonWmsItem = new HashMap<>();
        nonWmsItem.put("leaseKey", AttributeValue.fromS("some-shard-lease"));
        nonWmsItem.put("entityType", AttributeValue.fromS("LEASE"));
        nonWmsItem.put("lut", AttributeValue.fromN(String.valueOf(Instant.now().getEpochSecond())));
        ddbClient.putItem(b -> b.tableName(LEASE_TABLE).item(nonWmsItem)).get();

        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(false);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        List<WorkerMetricStats> results = dao.getAllWorkerMetricStats();

        assertTrue(results.stream().anyMatch(s -> "wms-entry".equals(s.getWorkerId())));
        assertFalse(results.stream().anyMatch(s -> "some-shard-lease".equals(s.getWorkerId())));
    }

    @Test
    void getAllWorkerMetricStats_legacyTable_worksWithoutEntityTypeAttribute() throws Exception {
        // During migration, old workers will write worker metric stats without the entityType
        // attribute, so the legacy scan must be able to read them without filtering on entityType.
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("wid", AttributeValue.fromS("legacy-no-et"));
        item.put("lut", AttributeValue.fromN(String.valueOf(Instant.now().getEpochSecond())));
        item.put(
                "sts",
                AttributeValue.fromM(ImmutableMap.of(
                        "C",
                        AttributeValue.builder().l(AttributeValue.fromN("50.0")).build())));
        item.put(
                "opr",
                AttributeValue.fromM(ImmutableMap.of(
                        "C",
                        AttributeValue.builder().l(AttributeValue.fromN("80")).build())));
        ddbClient.putItem(b -> b.tableName(LEGACY_TABLE).item(item)).get();

        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        WorkerMetricStatsDAO dao = createDao();
        dao.initialize();

        List<WorkerMetricStats> results = dao.getAllWorkerMetricStats();

        assertTrue(results.stream().anyMatch(s -> "legacy-no-et".equals(s.getWorkerId())));
    }

    @Test
    void getAllWorkerMetricStats_legacyDisabled_returnsEmptyList() throws Exception {
        // Use a table name that doesn't exist to ensure legacy is disabled
        WorkerMetricsTableConfig workerMetricsConfig = new WorkerMetricsTableConfig(null);
        workerMetricsConfig.tableName("nonexistent-table");

        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        when(statusProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        WorkerMetricStatsDAO dao =
                new WorkerMetricStatsDAO(ddbClient, workerMetricsConfig, LEASE_TABLE, 10000L, statusProvider);
        dao.initialize();

        // Legacy delegate should be disabled (table doesn't exist) and return empty
        List<WorkerMetricStats> legacyResults = dao.getLegacyTableDaoDelegate().getAllWorkerMetricStats();
        assertTrue(legacyResults.isEmpty());
    }

    // --- Helpers ---

    private void putWorkerMetricItem(String tableName, String pkAttr, String workerId) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(pkAttr, AttributeValue.fromS(workerId));
        item.put("wid", AttributeValue.fromS(workerId));
        item.put("lut", AttributeValue.fromN(String.valueOf(Instant.now().getEpochSecond())));
        item.put("entityType", AttributeValue.fromS(EntityType.WORKER_METRIC_STATS.getDdbValue()));
        item.put(
                "sts",
                AttributeValue.fromM(ImmutableMap.of(
                        "C",
                        AttributeValue.builder().l(AttributeValue.fromN("50.0")).build())));
        item.put(
                "opr",
                AttributeValue.fromM(ImmutableMap.of(
                        "C",
                        AttributeValue.builder().l(AttributeValue.fromN("80")).build())));
        try {
            ddbClient.putItem(b -> b.tableName(tableName).item(item)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, AttributeValue> getItem(String tableName, String pkAttr, String pkValue) {
        try {
            return ddbClient
                    .getItem(GetItemRequest.builder()
                            .tableName(tableName)
                            .key(Collections.singletonMap(pkAttr, AttributeValue.fromS(pkValue)))
                            .build())
                    .get()
                    .item();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
