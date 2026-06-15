package software.amazon.kinesis.coordinator.migration;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for TableMigrationStateMachineImpl using in-memory DynamoDB.
 * Verifies initialization logic, status determination, and state transitions.
 */
class TableMigrationStateMachineImplTest {

    private static final String LEGACY_TABLE = "tmsm-test-CoordinatorState";
    private static final String LEASE_TABLE = "tmsm-test-LeaseTable";
    private static final String WORKER_ID = "test-worker";

    private static AmazonDynamoDBLocal embeddedDdb;
    private static DynamoDbAsyncClient ddbClient;

    private TableMigrationStatusProvider statusProvider;

    @BeforeAll
    static void startDdb() throws Exception {
        embeddedDdb = DynamoDBEmbedded.create();
        ddbClient = embeddedDdb.dynamoDbAsyncClient();

        // Legacy coordinator state table
        ddbClient
                .createTable(CreateTableRequest.builder()
                        .tableName(LEGACY_TABLE)
                        .keySchema(KeySchemaElement.builder()
                                .attributeName("key")
                                .keyType(KeyType.HASH)
                                .build())
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("key")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build())
                        .build())
                .get();

        // Lease table
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
        statusProvider = new TableMigrationStatusProvider();
        // Clean up migration state from both tables to prevent cross-test contamination
        deleteItem(LEGACY_TABLE, "key", TableMigrationState.TABLE_MIGRATION_HASH_KEY);
        deleteItem(LEASE_TABLE, "leaseKey", TableMigrationState.TABLE_MIGRATION_HASH_KEY);
    }

    private void deleteItem(String tableName, String keyAttr, String keyValue) {
        try {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(keyAttr, AttributeValue.fromS(keyValue));
            ddbClient.deleteItem(b -> b.tableName(tableName).key(key)).get();
        } catch (Exception e) {
            // Ignore - item may not exist
        }
    }

    private CoordinatorStateDAO createCoordinatorStateDAO() {
        CoordinatorStateTableConfig tableConfig = new CoordinatorConfig("test-app").coordinatorStateTableConfig();
        tableConfig.tableName(LEGACY_TABLE);

        return new CoordinatorStateDAO(ddbClient, tableConfig, LEASE_TABLE, statusProvider);
    }

    private CoordinatorConfig createCoordinatorConfig(boolean migrateAll) {
        CoordinatorConfig config = new CoordinatorConfig(null);
        config.migrateAllEntitiesToLeaseTable(migrateAll);
        return config;
    }

    // --- Initialization: No legacy table (2→3 migration) ---

    @Test
    void initialize_noLegacyTable_shortCircuitsToComplete() throws Exception {
        // Use a non-existent legacy table to simulate 2→3 migration
        CoordinatorStateTableConfig tableConfig = new CoordinatorConfig("test-app").coordinatorStateTableConfig();
        tableConfig.tableName("non-existent-table");

        CoordinatorStateDAO dao = new CoordinatorStateDAO(ddbClient, tableConfig, LEASE_TABLE, statusProvider);
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, statusProvider.getTableMigrationStatus());
    }

    @Test
    void initialize_noLegacyTable_doesNotPersistStateToDDB() throws Exception {
        // 2→3 short-circuit should NOT write state to lease table — checking if the legacy
        // table exists is cheap so we just keep COMPLETE in memory on every startup.
        CoordinatorStateTableConfig tableConfig = new CoordinatorConfig("test-app").coordinatorStateTableConfig();
        tableConfig.tableName("non-existent-table");

        CoordinatorStateDAO dao = new CoordinatorStateDAO(ddbClient, tableConfig, LEASE_TABLE, statusProvider);
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        // Verify nothing was written to the lease table
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("leaseKey", AttributeValue.fromS(TableMigrationState.TABLE_MIGRATION_HASH_KEY));
        GetItemResponse result = ddbClient
                .getItem(b -> b.tableName(LEASE_TABLE).key(key).consistentRead(true))
                .get();
        assertTrue(
                result.item() == null || result.item().isEmpty(),
                "2→3 short-circuit should not persist state to lease table");
    }

    // --- Initialization: Legacy table exists, no state in DDB, config=false ---

    @Test
    void initialize_legacyTableExists_noState_configFalse_setsInit() throws Exception {
        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, statusProvider.getTableMigrationStatus());
    }

    // --- Initialization: Legacy table exists, no state in DDB, config=true ---

    @Test
    void initialize_legacyTableExists_noState_configTrue_throwsInvalidState() {
        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(true);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);

        assertThrows(InvalidStateException.class, sm::initialize);
    }

    // --- Initialization: DDB has DEPLOYED state, config=false → DEPLOYED ---

    @Test
    void initialize_ddbHasDeployed_configFalse_setsDeployed() throws Exception {
        // Pre-seed a DEPLOYED state in legacy table
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, statusProvider.getTableMigrationStatus());
    }

    // --- Initialization: DDB has DEPLOYED state, config=true → PENDING ---

    @Test
    void initialize_ddbHasDeployed_configTrue_setsPending() throws Exception {
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(true);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, statusProvider.getTableMigrationStatus());
    }

    // --- Initialization: DDB has PENDING state, config=false → DEPLOYED (rollback) ---

    @Test
    void initialize_ddbHasPending_configFalse_setsDeployed() throws Exception {
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING);

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, statusProvider.getTableMigrationStatus());
    }

    // --- Initialization: DDB has COMPLETE → always COMPLETE regardless of config ---

    @Test
    void initialize_ddbHasComplete_alwaysComplete() throws Exception {
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, statusProvider.getTableMigrationStatus());
    }

    // --- Initialization idempotency ---

    @Test
    void initialize_calledTwice_isIdempotent() throws Exception {
        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();
        sm.initialize(); // should not throw

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, statusProvider.getTableMigrationStatus());
    }

    // --- handleLeaderLockResult: not initialized → throws ---

    @Test
    void handleLeaderLockResult_beforeInit_throws() {
        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);

        assertThrows(IllegalStateException.class, () -> sm.handleLeaderLockResult(true));
    }

    // --- handleLeaderLockResult: COMPLETE status → no-op ---

    @Test
    void handleLeaderLockResult_whenComplete_isNoOp() throws Exception {
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        // Should not throw or change state
        sm.handleLeaderLockResult(true);
        sm.handleLeaderLockResult(false);
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, statusProvider.getTableMigrationStatus());
    }

    // --- handleLeaderLockResult: not leader → no state change ---

    @Test
    void handleLeaderLockResult_notLeader_noStateChange() throws Exception {
        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        sm.handleLeaderLockResult(false);
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, statusProvider.getTableMigrationStatus());
    }

    // --- handleLeaderLockResult: INIT + leader + no summary → no state change ---

    @Test
    void handleLeaderLockResult_initState_leader_noSummary_noStateChange() throws Exception {
        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        // No migration summary has been pushed — min support code check is conservative (false)
        sm.handleLeaderLockResult(true);
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, statusProvider.getTableMigrationStatus());
    }

    // --- handleLeaderLockResult: INIT + leader + min support code met → writes INIT to DDB ---

    @Test
    void handleLeaderLockResult_initState_leader_minSupportMet_writesInitToDDB() throws Exception {
        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        // Push a summary with min support code >= SINGLE_TABLE_MIGRATION ordinal
        sm.updateMigrationSummary(TableMigrationSummary.builder()
                .minSupportCode(1) // SINGLE_TABLE_MIGRATION.ordinal() == 1
                .workersWithUnexpiredLeases(1)
                .totalActiveWorkersWithMetrics(1)
                .activeWorkersWithMetricsInLegacyTable(0)
                .build());

        sm.handleLeaderLockResult(true);

        // Status remains INIT locally (bake time hasn't started or elapsed yet)
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, statusProvider.getTableMigrationStatus());

        // Verify state was written to legacy DDB
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.fromS(TableMigrationState.TABLE_MIGRATION_HASH_KEY));
        GetItemResponse result = ddbClient
                .getItem(b -> b.tableName(LEGACY_TABLE).key(key).consistentRead(true))
                .get();
        assertEquals(
                TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT.name(),
                result.item()
                        .get(TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME)
                        .s());
    }

    // --- handleLeaderLockResult: INIT + bake time elapsed → transitions to DEPLOYED ---

    @Test
    void handleLeaderLockResult_initState_bakeTimeElapsed_transitionsToDeployed() throws Exception {
        // Pre-seed INIT state with old timestamp (bake time already elapsed)
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("key", AttributeValue.fromS(TableMigrationState.TABLE_MIGRATION_HASH_KEY));
        item.put(
                TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME,
                AttributeValue.fromS(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT.name()));
        item.put(TableMigrationState.MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS(WORKER_ID));
        // Set timestamp far in the past so bake time is elapsed
        item.put(TableMigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN("0"));
        ddbClient.putItem(b -> b.tableName(LEGACY_TABLE).item(item)).get();

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        // Push summary with min support code met
        sm.updateMigrationSummary(TableMigrationSummary.builder()
                .minSupportCode(1)
                .workersWithUnexpiredLeases(1)
                .totalActiveWorkersWithMetrics(1)
                .activeWorkersWithMetricsInLegacyTable(0)
                .build());

        sm.handleLeaderLockResult(true);

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, statusProvider.getTableMigrationStatus());
    }

    // --- handleLeaderLockResult: DEPLOYED + DDB=PENDING + config=false → writes DEPLOYED back ---

    @Test
    void handleLeaderLockResult_deployedState_ddbPending_configFalse_rollsBackToDeployed() throws Exception {
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING);

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, statusProvider.getTableMigrationStatus());

        sm.handleLeaderLockResult(true);

        // Verify DDB was rolled back to DEPLOYED
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.fromS(TableMigrationState.TABLE_MIGRATION_HASH_KEY));
        GetItemResponse result = ddbClient
                .getItem(b -> b.tableName(LEGACY_TABLE).key(key).consistentRead(true))
                .get();
        assertEquals(
                TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED.name(),
                result.item()
                        .get(TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME)
                        .s());
    }

    // --- handleLeaderLockResult: another leader completes migration → throws InvalidStateException ---

    @Test
    void handleLeaderLockResult_anotherLeaderCompletedMigration_throwsInvalidState() throws Exception {
        // Start in DEPLOYED state
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(false);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        // Simulate another leader writing COMPLETE to DDB
        deleteItem(LEGACY_TABLE, "key", TableMigrationState.TABLE_MIGRATION_HASH_KEY);
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // handleLeaderLockResult should detect COMPLETE and throw
        assertThrows(InvalidStateException.class, () -> sm.handleLeaderLockResult(true));
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, statusProvider.getTableMigrationStatus());
    }

    // --- handleLeaderLockResult: PENDING + DDB=PENDING + legacy non-empty → rollback to DEPLOYED ---

    @Test
    void handleLeaderLockResult_pendingState_legacyNonEmpty_rollsBackToDeployed() throws Exception {
        putTableMigrationState(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING);

        CoordinatorStateDAO dao = createCoordinatorStateDAO();
        CoordinatorConfig coordConfig = createCoordinatorConfig(true);

        TableMigrationStateMachineImpl sm =
                new TableMigrationStateMachineImpl(statusProvider, dao, WORKER_ID, coordConfig);
        sm.initialize();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, statusProvider.getTableMigrationStatus());

        // Push summary indicating legacy is NOT empty (rollback detected)
        sm.updateMigrationSummary(TableMigrationSummary.builder()
                .minSupportCode(1)
                .workersWithUnexpiredLeases(2)
                .totalActiveWorkersWithMetrics(2)
                .activeWorkersWithMetricsInLegacyTable(1) // non-empty
                .build());

        sm.handleLeaderLockResult(true);

        // Verify DDB was rolled back to DEPLOYED
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.fromS(TableMigrationState.TABLE_MIGRATION_HASH_KEY));
        GetItemResponse result = ddbClient
                .getItem(b -> b.tableName(LEGACY_TABLE).key(key).consistentRead(true))
                .get();
        assertEquals(
                TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED.name(),
                result.item()
                        .get(TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME)
                        .s());
    }

    // --- Helpers ---

    private void putTableMigrationState(TableMigrationStatus status) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("key", AttributeValue.fromS(TableMigrationState.TABLE_MIGRATION_HASH_KEY));
        item.put(TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME, AttributeValue.fromS(status.name()));
        item.put(TableMigrationState.MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS(WORKER_ID));
        item.put(
                TableMigrationState.MODIFIED_TIMESTAMP_ATTRIBUTE_NAME,
                AttributeValue.fromN(String.valueOf(System.currentTimeMillis())));
        try {
            ddbClient.putItem(b -> b.tableName(LEGACY_TABLE).item(item)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
