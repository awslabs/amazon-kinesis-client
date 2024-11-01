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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.internal.waiters.DefaultWaiterResponse;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.coordinator.migration.ClientVersion;
import software.amazon.kinesis.coordinator.migration.MigrationState;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.coordinator.CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME;
import static software.amazon.kinesis.coordinator.CoordinatorState.LEADER_HASH_KEY;
import static software.amazon.kinesis.coordinator.migration.MigrationState.CLIENT_VERSION_ATTRIBUTE_NAME;
import static software.amazon.kinesis.coordinator.migration.MigrationState.MIGRATION_HASH_KEY;

@Slf4j
public class CoordinatorStateDAOTest {
    private static final String WORKER_ID = "CoordinatorStateDAOTestWorker";
    private final AmazonDynamoDBLocal embeddedDdb = DynamoDBEmbedded.create();
    private final DynamoDbAsyncClient dynamoDbAsyncClient = embeddedDdb.dynamoDbAsyncClient();
    private String tableNameForTest;

    @Test
    public void testProvisionedTableCreation_DefaultTableName()
            throws ExecutionException, InterruptedException, DependencyException {
        /* Test setup - create class under test **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient,
                getCoordinatorStateConfig(
                        "testProvisionedTableCreation",
                        ProvisionedThroughput.builder()
                                .writeCapacityUnits(30L)
                                .readCapacityUnits(15L)
                                .build()));

        /* Test step - initialize to create the table **/
        doaUnderTest.initialize();

        /* Verify - table with correct configuration is created */
        final DescribeTableResponse response = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName("testProvisionedTableCreation-CoordinatorState")
                        .build())
                .get();

        Assertions.assertEquals(
                15L,
                response.table().provisionedThroughput().readCapacityUnits().longValue());
        Assertions.assertEquals(
                30L,
                response.table().provisionedThroughput().writeCapacityUnits().longValue());
    }

    @Test
    public void testTableCreationWithDeletionProtection_assertDeletionProtectionEnabled()
            throws DependencyException, ExecutionException, InterruptedException {

        final CoordinatorStateTableConfig config = getCoordinatorStateConfig(
                "testTableCreationWithDeletionProtection",
                ProvisionedThroughput.builder()
                        .writeCapacityUnits(30L)
                        .readCapacityUnits(15L)
                        .build());
        config.deletionProtectionEnabled(true);
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(dynamoDbAsyncClient, config);

        doaUnderTest.initialize();

        final DescribeTableResponse response = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName("testTableCreationWithDeletionProtection-CoordinatorState")
                        .build())
                .get();

        Assertions.assertTrue(response.table().deletionProtectionEnabled());
    }

    /**
     * DynamoDBLocal does not support PITR and tags and thus this test is using mocks.
     */
    @Test
    public void testTableCreationWithTagsAndPitr_assertTags() throws DependencyException {
        final DynamoDbAsyncWaiter waiter = mock(DynamoDbAsyncWaiter.class);
        final WaiterResponse<?> waiterResponse = DefaultWaiterResponse.builder()
                .response(dummyDescribeTableResponse(TableStatus.ACTIVE))
                .attemptsExecuted(1)
                .build();
        when(waiter.waitUntilTableExists(any(Consumer.class), any(Consumer.class)))
                .thenReturn(CompletableFuture.completedFuture((WaiterResponse<DescribeTableResponse>) waiterResponse));
        final DynamoDbAsyncClient dbAsyncClient = mock(DynamoDbAsyncClient.class);
        when(dbAsyncClient.waiter()).thenReturn(waiter);
        when(dbAsyncClient.createTable(any(CreateTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(CreateTableResponse.builder()
                        .tableDescription(
                                dummyDescribeTableResponse(TableStatus.CREATING).table())
                        .build()));
        when(dbAsyncClient.updateContinuousBackups(any(UpdateContinuousBackupsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(dbAsyncClient.describeTable(any(DescribeTableRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().build())
                .thenReturn(CompletableFuture.completedFuture(dummyDescribeTableResponse(TableStatus.ACTIVE)));

        final ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor =
                ArgumentCaptor.forClass(CreateTableRequest.class);
        final ArgumentCaptor<UpdateContinuousBackupsRequest> updateContinuousBackupsRequestArgumentCaptor =
                ArgumentCaptor.forClass(UpdateContinuousBackupsRequest.class);

        final CoordinatorStateTableConfig config = getCoordinatorStateConfig(
                "testTableCreationWithTagsAndPitr",
                ProvisionedThroughput.builder()
                        .writeCapacityUnits(30L)
                        .readCapacityUnits(15L)
                        .build());
        config.tableName("testTableCreationWithTagsAndPitr");
        config.pointInTimeRecoveryEnabled(true);
        config.tags(
                Collections.singleton(Tag.builder().key("Key").value("Value").build()));

        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(dbAsyncClient, config);
        doaUnderTest.initialize();

        verify(dbAsyncClient).createTable(createTableRequestArgumentCaptor.capture());
        verify(dbAsyncClient).updateContinuousBackups(updateContinuousBackupsRequestArgumentCaptor.capture());
        Assertions.assertEquals(
                1, createTableRequestArgumentCaptor.getValue().tags().size());

        Assertions.assertEquals(
                "Key", createTableRequestArgumentCaptor.getValue().tags().get(0).key());
        Assertions.assertEquals(
                "Value",
                createTableRequestArgumentCaptor.getValue().tags().get(0).value());
        Assertions.assertTrue(updateContinuousBackupsRequestArgumentCaptor
                .getAllValues()
                .get(0)
                .pointInTimeRecoverySpecification()
                .pointInTimeRecoveryEnabled());
    }

    private static DescribeTableResponse dummyDescribeTableResponse(final TableStatus tableStatus) {
        return DescribeTableResponse.builder()
                .table(TableDescription.builder().tableStatus(tableStatus).build())
                .build();
    }

    @Test
    public void testPayPerUseTableCreation_DefaultTableName()
            throws ExecutionException, InterruptedException, DependencyException {
        /* Test setup - create class under test **/
        final CoordinatorConfig c = new CoordinatorConfig("testPayPerUseTableCreation");
        c.coordinatorStateTableConfig().billingMode(BillingMode.PAY_PER_REQUEST);

        final CoordinatorStateDAO doaUnderTest =
                new CoordinatorStateDAO(dynamoDbAsyncClient, c.coordinatorStateTableConfig());

        /* Test step - initialize to create the table **/
        doaUnderTest.initialize();

        /* Verify - table with correct configuration is created */
        final DescribeTableResponse response = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName("testPayPerUseTableCreation-CoordinatorState")
                        .build())
                .get();

        Assertions.assertEquals(
                BillingMode.PAY_PER_REQUEST,
                response.table().billingModeSummary().billingMode());
    }

    @Test
    public void testProvisionedTableCreation_CustomTableName()
            throws ExecutionException, InterruptedException, DependencyException {
        /* Test setup - create class under test **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient,
                getCoordinatorStateConfig(
                        "TestApplicationName",
                        BillingMode.PROVISIONED,
                        ProvisionedThroughput.builder()
                                .readCapacityUnits(10L)
                                .writeCapacityUnits(20L)
                                .build(),
                        "MyCustomTableName-testProvisionedTableCreation"));

        /* Test step - initialize to create the table **/
        doaUnderTest.initialize();

        /* Verify - table with correct configuration is created */
        final DescribeTableResponse response = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName("MyCustomTableName-testProvisionedTableCreation")
                        .build())
                .get();

        Assertions.assertEquals(
                10L,
                response.table().provisionedThroughput().readCapacityUnits().longValue());
        Assertions.assertEquals(
                20L,
                response.table().provisionedThroughput().writeCapacityUnits().longValue());
    }

    @Test
    public void testPayPerUseTableCreation_CustomTableName()
            throws ExecutionException, InterruptedException, DependencyException {
        /* Test setup - create class under test **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient,
                getCoordinatorStateConfig(
                        "TestApplicationName",
                        BillingMode.PAY_PER_REQUEST,
                        null,
                        "MyCustomTableName-testPayPerUseTableCreation"));

        /* Test step - initialize to create the table **/
        doaUnderTest.initialize();

        /* Verify - table with correct configuration is created */
        final DescribeTableResponse response = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName("MyCustomTableName-testPayPerUseTableCreation")
                        .build())
                .get();

        Assertions.assertEquals(
                BillingMode.PAY_PER_REQUEST,
                response.table().billingModeSummary().billingMode());
    }

    @Test
    public void testCreatingLeaderAndMigrationKey()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException, InterruptedException,
                    IOException {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();

        /* Test steps - create migration item, DDB lease election lock item, and another item with different schema **/
        createCoordinatorState("key1");

        final MigrationState migrationState = new MigrationState(MIGRATION_HASH_KEY, WORKER_ID)
                .update(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, WORKER_ID);
        doaUnderTest.createCoordinatorStateIfNotExists(migrationState);

        final AmazonDynamoDBLockClient dynamoDBLockClient = new AmazonDynamoDBLockClient(doaUnderTest
                .getDDBLockClientOptionsBuilder()
                .withOwnerName("TEST_WORKER")
                .withCreateHeartbeatBackgroundThread(true)
                .build());
        final Optional<LockItem> optionalItem = dynamoDBLockClient.tryAcquireLock(
                AcquireLockOptions.builder(LEADER_HASH_KEY).build());
        Assertions.assertTrue(optionalItem.isPresent(), "Lock was not acquired");

        final AmazonDynamoDBLockClient worker2DynamoDBLockClient = new AmazonDynamoDBLockClient(doaUnderTest
                .getDDBLockClientOptionsBuilder()
                .withOwnerName("TEST_WORKER_2")
                .withCreateHeartbeatBackgroundThread(true)
                .build());
        final Optional<LockItem> worker2OptionalItem = worker2DynamoDBLockClient.tryAcquireLock(
                AcquireLockOptions.builder(LEADER_HASH_KEY).build());
        Assertions.assertFalse(worker2OptionalItem.isPresent(), "Second worker was able to acquire the lock");

        /* Verify - both items are present with the corresponding content */
        final ScanResponse response = FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.scan(
                ScanRequest.builder().tableName(tableNameForTest).build()));
        log.info("response {}", response);

        Assertions.assertEquals(3, response.scannedCount(), "incorrect item count");
        response.items().forEach(item -> {
            final String key =
                    item.get(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME).s();
            if (MIGRATION_HASH_KEY.equals(key)) {
                // Make sure the record has not changed due to using
                // ddb lock client
                Assertions.assertEquals(
                        ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X.toString(),
                        item.get(CLIENT_VERSION_ATTRIBUTE_NAME).s());
            } else if (LEADER_HASH_KEY.equals(key)) {
                Assertions.assertEquals("TEST_WORKER", item.get("ownerName").s());
            } else if ("key1".equals(key)) {
                Assertions.assertEquals(4, item.size());
                Assertions.assertEquals("key1_strVal", item.get("key1-StrAttr").s());
                Assertions.assertEquals(
                        100, Integer.valueOf(item.get("key1-IntAttr").n()));
                Assertions.assertEquals(true, item.get("key1-BoolAttr").bool());
            }
        });

        dynamoDBLockClient.close();
        worker2DynamoDBLockClient.close();
    }

    @Test
    public void testListCoordinatorState()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest =
                new CoordinatorStateDAO(dynamoDbAsyncClient, getCoordinatorStateConfig("testListCoordinatorState"));
        doaUnderTest.initialize();

        /* Test step - create a few coordinatorState items with different schema and invoke the test to list items */
        createCoordinatorState("key1");
        createCoordinatorState("key2");
        createCoordinatorState("key3");
        createCoordinatorState("key4");
        createMigrationState();

        final List<CoordinatorState> stateList = doaUnderTest.listCoordinatorState();

        /* Verify **/
        Assertions.assertEquals(5, stateList.size());
        stateList.forEach(state -> {
            final String keyValue = state.getKey();
            if ("Migration3.0".equals(keyValue)) {
                Assertions.assertTrue(state instanceof MigrationState);
                final MigrationState migrationState = (MigrationState) state;
                Assertions.assertEquals(ClientVersion.CLIENT_VERSION_3X, migrationState.getClientVersion());
                return;
            }
            Assertions.assertEquals(3, state.getAttributes().size());
            Assertions.assertEquals(
                    keyValue + "_strVal",
                    state.getAttributes().get(keyValue + "-StrAttr").s());
            Assertions.assertEquals(
                    100,
                    Integer.valueOf(
                            state.getAttributes().get(keyValue + "-IntAttr").n()));
            Assertions.assertEquals(
                    true, state.getAttributes().get(keyValue + "-BoolAttr").bool());
        });
    }

    @Test
    public void testCreateCoordinatorState_ItemNotExists()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();

        /* Test step - create a few coordinatorState items with different schema and invoke the test to list items */
        final CoordinatorState s1 = CoordinatorState.builder()
                .key("key1")
                .attributes(new HashMap<String, AttributeValue>() {
                    {
                        put("abc", AttributeValue.fromS("abc"));
                        put("xyz", AttributeValue.fromS("xyz"));
                    }
                })
                .build();
        final boolean result = doaUnderTest.createCoordinatorStateIfNotExists(s1);

        /* Verify - insert succeeded and item matches **/
        Assertions.assertTrue(result);
        final CoordinatorState stateFromDdb = doaUnderTest.getCoordinatorState("key1");
        Assertions.assertEquals(s1, stateFromDdb);
    }

    @Test
    public void testCreateCoordinatorState_ItemExists()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();
        createCoordinatorState("key1");

        /* Test step - create a few coordinatorState items with different schema and invoke the test to list items */
        final CoordinatorState s1 = CoordinatorState.builder()
                .key("key1")
                .attributes(new HashMap<String, AttributeValue>() {
                    {
                        put("abc", AttributeValue.fromS("abc"));
                        put("xyz", AttributeValue.fromS("xyz"));
                    }
                })
                .build();
        final boolean result = doaUnderTest.createCoordinatorStateIfNotExists(s1);

        /* Verify - insert succeeded and item matches **/
        Assertions.assertFalse(result);
        final CoordinatorState stateFromDdb = doaUnderTest.getCoordinatorState("key1");
        Assertions.assertNotEquals(s1, stateFromDdb);
    }

    @Test
    public void testUpdateCoordinatorStateWithExpectation_Success()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();
        createCoordinatorState("key1");

        /* Test step - update the state */
        final CoordinatorState updatedState = CoordinatorState.builder()
                .key("key1")
                .attributes(new HashMap<String, AttributeValue>() {
                    {
                        put("key1-StrAttr", AttributeValue.fromS("key1_strVal"));
                        put("key1-IntAttr", AttributeValue.fromN("200"));
                        put("key1-BoolAttr", AttributeValue.fromBool(false));
                    }
                })
                .build();

        final boolean updated = doaUnderTest.updateCoordinatorStateWithExpectation(
                updatedState, new HashMap<String, ExpectedAttributeValue>() {
                    {
                        put(
                                "key1-StrAttr",
                                ExpectedAttributeValue.builder()
                                        .value(AttributeValue.fromS("key1_strVal"))
                                        .build());
                    }
                });

        /* Verify - update succeeded **/
        Assertions.assertTrue(updated);
    }

    @Test
    public void testUpdateCoordinatorStateWithExpectation_ConditionFailed()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();
        final MigrationState state = createMigrationState();

        /* Test step - update the state with mismatched condition */
        final MigrationState updatedState = state.copy().update(ClientVersion.CLIENT_VERSION_2X, WORKER_ID);

        boolean updated = doaUnderTest.updateCoordinatorStateWithExpectation(
                updatedState, updatedState.getDynamoClientVersionExpectation());

        /* Verify - update failed **/
        Assertions.assertFalse(updated);

        /* Verify - update succeeded **/
        final MigrationState currentState = (MigrationState) doaUnderTest.getCoordinatorState("Migration3.0");
        updated = doaUnderTest.updateCoordinatorStateWithExpectation(
                updatedState, currentState.getDynamoClientVersionExpectation());
        Assertions.assertTrue(updated);
        final GetItemResponse response = dynamoDbAsyncClient
                .getItem(GetItemRequest.builder()
                        .tableName(tableNameForTest)
                        .key(new HashMap<String, AttributeValue>() {
                            {
                                put("key", AttributeValue.fromS("Migration3.0"));
                            }
                        })
                        .build())
                .join();
        Assertions.assertEquals(
                ClientVersion.CLIENT_VERSION_2X.name(),
                response.item().get("cv").s());
        Assertions.assertEquals(WORKER_ID, response.item().get("mb").s());
        Assertions.assertEquals(
                String.valueOf(updatedState.getModifiedTimestamp()),
                response.item().get("mts").n());
        Assertions.assertEquals(1, response.item().get("h").l().size());
        Assertions.assertEquals(
                state.getClientVersion().name(),
                response.item().get("h").l().get(0).m().get("cv").s());
        Assertions.assertEquals(
                state.getModifiedBy(),
                response.item().get("h").l().get(0).m().get("mb").s());
        Assertions.assertEquals(
                String.valueOf(state.getModifiedTimestamp()),
                response.item().get("h").l().get(0).m().get("mts").n());

        log.info("Response {}", response);
    }

    @Test
    public void testUpdateCoordinatorStateWithExpectation_NonExistentKey()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = new CoordinatorStateDAO(
                dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();

        /* Test step - update with new state object */
        final MigrationState updatedState =
                new MigrationState("Migration3.0", WORKER_ID).update(ClientVersion.CLIENT_VERSION_2X, WORKER_ID);

        boolean updated = doaUnderTest.updateCoordinatorStateWithExpectation(updatedState, null);

        /* Verify - update failed **/
        Assertions.assertFalse(updated);
    }

    private CoordinatorStateTableConfig getCoordinatorStateConfig(final String applicationName) {
        return getCoordinatorStateConfig(applicationName, BillingMode.PAY_PER_REQUEST, null, null);
    }

    private CoordinatorStateTableConfig getCoordinatorStateConfig(
            final String applicationName, final ProvisionedThroughput throughput) {
        return getCoordinatorStateConfig(applicationName, BillingMode.PROVISIONED, throughput, null);
    }

    private CoordinatorStateTableConfig getCoordinatorStateConfig(
            final String applicationName,
            final BillingMode mode,
            final ProvisionedThroughput throughput,
            final String tableName) {
        final CoordinatorConfig c = new CoordinatorConfig(applicationName);
        c.coordinatorStateTableConfig().billingMode(mode);
        if (tableName != null) {
            c.coordinatorStateTableConfig().tableName(tableName);
        }
        if (mode == BillingMode.PROVISIONED) {
            c.coordinatorStateTableConfig()
                    .writeCapacity(throughput.writeCapacityUnits())
                    .readCapacity(throughput.readCapacityUnits());
        }

        tableNameForTest = c.coordinatorStateTableConfig().tableName();

        return c.coordinatorStateTableConfig();
    }

    private void createCoordinatorState(final String keyValue) {
        dynamoDbAsyncClient
                .putItem(PutItemRequest.builder()
                        .tableName(tableNameForTest)
                        .item(new HashMap<String, AttributeValue>() {
                            {
                                put("key", AttributeValue.fromS(keyValue));
                                put(keyValue + "-StrAttr", AttributeValue.fromS(keyValue + "_strVal"));
                                put(keyValue + "-IntAttr", AttributeValue.fromN("100"));
                                put(keyValue + "-BoolAttr", AttributeValue.fromBool(true));
                            }
                        })
                        .build())
                .join();
    }

    private MigrationState createMigrationState() {
        final HashMap<String, AttributeValue> item = new HashMap<String, AttributeValue>() {
            {
                put("key", AttributeValue.fromS("Migration3.0"));
                put("cv", AttributeValue.fromS(ClientVersion.CLIENT_VERSION_3X.toString()));
                put("mb", AttributeValue.fromS("DUMMY_WORKER"));
                put("mts", AttributeValue.fromN(String.valueOf(System.currentTimeMillis())));
            }
        };

        dynamoDbAsyncClient
                .putItem(PutItemRequest.builder()
                        .tableName(tableNameForTest)
                        .item(item)
                        .build())
                .join();

        item.remove("key");

        return MigrationState.deserialize("Migration3.0", item);
    }
}
