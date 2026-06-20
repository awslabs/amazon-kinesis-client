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
package software.amazon.kinesis.coordinator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test for CoordinatorStateDAO routing using in-memory DynamoDB.
 * Verifies that reads/writes go to the correct table based on TableMigrationStatus.
 */
@Slf4j
class CoordinatorStateDAODelegateRoutingTest {

    private static final String LEGACY_TABLE = "routing-test-CoordinatorState";
    private static final String LEASE_TABLE = "routing-test-LeaseTable";
    private static final String PK = "key";

    private static AmazonDynamoDBLocal embeddedDdb;
    private static DynamoDbAsyncClient ddbClient;

    private TableMigrationStatusProvider statusProvider;

    @BeforeAll
    static void startDdb() throws Exception {
        embeddedDdb = DynamoDBEmbedded.create();
        ddbClient = embeddedDdb.dynamoDbAsyncClient();

        // Create legacy coordinator state table
        ddbClient
                .createTable(CreateTableRequest.builder()
                        .tableName(LEGACY_TABLE)
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .keySchema(KeySchemaElement.builder()
                                .attributeName(PK)
                                .keyType(KeyType.HASH)
                                .build())
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName(PK)
                                .attributeType(ScalarAttributeType.S)
                                .build())
                        .build())
                .get();

        // Create lease table
        ddbClient
                .createTable(CreateTableRequest.builder()
                        .tableName(LEASE_TABLE)
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .keySchema(KeySchemaElement.builder()
                                .attributeName("leaseKey")
                                .keyType(KeyType.HASH)
                                .build())
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("leaseKey")
                                .attributeType(ScalarAttributeType.S)
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
    }

    private CoordinatorStateDAO createDao() {
        CoordinatorStateTableConfig tableConfig = new CoordinatorConfig("test-app").coordinatorStateTableConfig();
        tableConfig.tableName(LEGACY_TABLE);

        return new CoordinatorStateDAO(ddbClient, tableConfig, LEASE_TABLE, statusProvider);
    }

    @Test
    void getCoordinatorState_whenComplete_readsFromLeaseTable()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        CoordinatorStateDAO dao = createDao();
        dao.initializeDelegates();
        dao.initialize();

        // Write directly to lease table
        putItem(
                LEASE_TABLE,
                "leaseKey",
                "test-key-1",
                Collections.singletonMap(
                        "entityType", AttributeValue.fromS(EntityType.CoordinatorStateType.STREAM_INFO.getDdbValue())));

        CoordinatorState result = dao.getCoordinatorState("test-key-1");
        assertNotNull(result);
        assertEquals("test-key-1", result.getKey());
    }

    @Test
    void getCoordinatorState_whenInit_readsFromLegacyFirst()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        CoordinatorStateDAO dao = createDao();
        dao.initializeDelegates();
        dao.initialize();

        // Write to legacy table
        putItem(LEGACY_TABLE, PK, "legacy-key", Collections.singletonMap("entityType", AttributeValue.fromS("STREAM")));

        CoordinatorState result = dao.getCoordinatorState("legacy-key");
        assertNotNull(result);
        assertEquals("legacy-key", result.getKey());
    }

    @Test
    void getCoordinatorState_whenInit_fallsBackToLeaseTable()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        CoordinatorStateDAO dao = createDao();
        dao.initializeDelegates();
        dao.initialize();

        // Only write to lease table (not in legacy)
        putItem(
                LEASE_TABLE,
                "leaseKey",
                "only-in-lease",
                Collections.singletonMap(
                        "entityType", AttributeValue.fromS(EntityType.CoordinatorStateType.STREAM_INFO.getDdbValue())));

        CoordinatorState result = dao.getCoordinatorState("only-in-lease");
        assertNotNull(result);
        assertEquals("only-in-lease", result.getKey());
    }

    @Test
    void writeOperation_beforeInitialize_throwsInvalidStateException() throws DependencyException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        CoordinatorStateDAO dao = createDao();
        dao.initializeDelegates();
        // Don't call dao.initialize()

        CoordinatorState state = CoordinatorState.builder().key("test-key").build();
        assertThrows(InvalidStateException.class, () -> dao.createCoordinatorStateIfNotExists(state));
    }

    @Test
    void initialize_whenStatusUnknown_throwsInvalidStateException() throws DependencyException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_UNKNOWN);
        CoordinatorStateDAO dao = createDao();
        dao.initializeDelegates();
        assertThrows(InvalidStateException.class, dao::initialize);
    }

    @Test
    void createCoordinatorState_whenInit_writesToLegacy()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        CoordinatorStateDAO dao = createDao();
        dao.initializeDelegates();
        dao.initialize();
        dao.initialize();

        CoordinatorState state = CoordinatorState.builder()
                .key("write-test-init")
                .coordinatorStateEntityType(EntityType.CoordinatorStateType.STREAM_INFO)
                .build();
        boolean created = dao.createCoordinatorStateIfNotExists(state);
        assertTrue(created);

        // Verify it's in legacy table
        Map<String, AttributeValue> item = getItem(LEGACY_TABLE, PK, "write-test-init");
        assertNotNull(item);
        assertFalse(item.isEmpty());
    }

    @Test
    void createCoordinatorState_whenComplete_writesToLeaseTable()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        CoordinatorStateDAO dao = createDao();
        dao.initializeDelegates();
        dao.initialize();
        dao.initialize();

        CoordinatorState state = CoordinatorState.builder()
                .key("write-test-complete")
                .coordinatorStateEntityType(EntityType.CoordinatorStateType.STREAM_INFO)
                .build();
        boolean created = dao.createCoordinatorStateIfNotExists(state);
        assertTrue(created);

        // Verify it's in lease table
        Map<String, AttributeValue> item = getItem(LEASE_TABLE, "leaseKey", "write-test-complete");
        assertNotNull(item);
        assertFalse(item.isEmpty());
    }

    @Test
    void createCoordinatorState_whenPending_writesToLeaseTable()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        when(statusProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING);
        CoordinatorStateDAO dao = createDao();
        dao.initializeDelegates();
        dao.initialize();
        dao.initialize();

        CoordinatorState state = CoordinatorState.builder()
                .key("write-test-pending")
                .coordinatorStateEntityType(EntityType.CoordinatorStateType.STREAM_INFO)
                .build();
        boolean created = dao.createCoordinatorStateIfNotExists(state);
        assertTrue(created);

        Map<String, AttributeValue> item = getItem(LEASE_TABLE, "leaseKey", "write-test-pending");
        assertNotNull(item);
        assertFalse(item.isEmpty());
    }

    // --- Helpers ---

    private void putItem(String tableName, String pkName, String pkValue, Map<String, AttributeValue> extraAttrs) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(pkName, AttributeValue.fromS(pkValue));
        item.putAll(extraAttrs);
        try {
            ddbClient.putItem(b -> b.tableName(tableName).item(item)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, AttributeValue> getItem(String tableName, String pkName, String pkValue) {
        try {
            return ddbClient
                    .getItem(b ->
                            b.tableName(tableName).key(Collections.singletonMap(pkName, AttributeValue.fromS(pkValue))))
                    .get()
                    .item();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
