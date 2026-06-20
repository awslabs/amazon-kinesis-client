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
import java.util.List;
import java.util.Optional;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.coordinator.migration.ClientVersion;
import software.amazon.kinesis.coordinator.migration.MigrationState;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfo;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.coordinator.migration.MigrationState.CLIENT_VERSION_ATTRIBUTE_NAME;
import static software.amazon.kinesis.coordinator.migration.MigrationState.MIGRATION_HASH_KEY;
import static software.amazon.kinesis.leader.LeaderLock.LEADER_HASH_KEY;

@Slf4j
public class CoordinatorStateDAOTest {
    private static final String WORKER_ID = "CoordinatorStateDAOTestWorker";
    private final AmazonDynamoDBLocal embeddedDdb = DynamoDBEmbedded.create();
    private final DynamoDbAsyncClient dynamoDbAsyncClient = embeddedDdb.dynamoDbAsyncClient();
    private static final String LEASE_TABLE_NAME = "TestLeaseTable";
    private static final String DUMMY_WORKER_ID = "DUMMY_WORKER";
    // To create Lease table
    private final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
            LEASE_TABLE_NAME,
            dynamoDbAsyncClient,
            new DynamoDBLeaseSerializer(),
            true,
            TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
            LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
            new DdbTableConfig(),
            LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
            LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
            DefaultSdkAutoConstructList.getInstance());

    @Test
    public void testCreatingLeaderAndMigrationKey() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest =
                createDAO(dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();

        /* Test steps - create migration item, DDB lease election lock item, and StremInfo **/
        StreamInfo streamInfo = new StreamInfo("stream1", "streamId1");
        doaUnderTest.createCoordinatorStateIfNotExists(streamInfo);

        final MigrationState migrationState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, WORKER_ID);
        doaUnderTest.createCoordinatorStateIfNotExists(migrationState);

        final AmazonDynamoDBLockClient dynamoDBLockClient = new AmazonDynamoDBLockClient(doaUnderTest
                .getLeaseTableDaoDelegate()
                .getDDBLockClientOptionsBuilder()
                .withOwnerName("TEST_WORKER")
                .withCreateHeartbeatBackgroundThread(true)
                .build());
        final Optional<LockItem> optionalItem =
                dynamoDBLockClient.tryAcquireLock(AcquireLockOptions.builder(LEADER_HASH_KEY)
                        .withAdditionalAttributes(Collections.singletonMap(
                                CoordinatorState.ENTITY_TYPE_ATTRIBUTE_NAME,
                                AttributeValue.fromS(EntityType.LEADER_LOCK.getDdbValue())))
                        .build());
        Assertions.assertTrue(optionalItem.isPresent(), "Lock was not acquired");

        final AmazonDynamoDBLockClient worker2DynamoDBLockClient = new AmazonDynamoDBLockClient(doaUnderTest
                .getLeaseTableDaoDelegate()
                .getDDBLockClientOptionsBuilder()
                .withOwnerName("TEST_WORKER_2")
                .withCreateHeartbeatBackgroundThread(true)
                .build());
        final Optional<LockItem> worker2OptionalItem = worker2DynamoDBLockClient.tryAcquireLock(
                AcquireLockOptions.builder(LEADER_HASH_KEY).build());
        Assertions.assertFalse(worker2OptionalItem.isPresent(), "Second worker was able to acquire the lock");

        /* Verify - all 3 items are present with the corresponding content */
        final ScanResponse response = FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.scan(
                ScanRequest.builder().tableName(LEASE_TABLE_NAME).build()));
        log.info("response {}", response);

        Assertions.assertEquals(3, response.scannedCount(), "incorrect item count");
        response.items().forEach(item -> {
            final String key = item.get(DynamoDBLeaseSerializer.LEASE_KEY_KEY).s();
            if (MIGRATION_HASH_KEY.equals(key)) {
                // Make sure the record has not changed due to using
                // ddb lock client
                Assertions.assertEquals(
                        ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X.toString(),
                        item.get(CLIENT_VERSION_ATTRIBUTE_NAME).s());
                Assertions.assertEquals(
                        EntityType.CLIENT_VERSION_MIGRATION.getDdbValue(),
                        item.get(CoordinatorState.ENTITY_TYPE_ATTRIBUTE_NAME).s());
            } else if (LEADER_HASH_KEY.equals(key)) {
                Assertions.assertEquals("TEST_WORKER", item.get("ownerName").s());
                Assertions.assertEquals(
                        EntityType.LEADER_LOCK.getDdbValue(),
                        item.get(CoordinatorState.ENTITY_TYPE_ATTRIBUTE_NAME).s());
            } else if ("stream1".equals(key)) {
                Assertions.assertEquals(3, item.size());
                Assertions.assertEquals(
                        "streamId1",
                        item.get(StreamInfo.STREAM_ID_ATTRIBUTE_NAME).s());
                Assertions.assertEquals(
                        EntityType.STREAM_INFO.getDdbValue(),
                        item.get(CoordinatorState.ENTITY_TYPE_ATTRIBUTE_NAME).s());
            }
        });

        dynamoDBLockClient.close();
        worker2DynamoDBLockClient.close();
    }

    @Test
    public void testListCoordinatorState() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = createDAO(getCoordinatorStateConfig("testListCoordinatorState"));
        doaUnderTest.initialize();

        /* Test step - create a few coordinatorState items with different schema and invoke the test to list items */
        createStreamInfoCoordinatorState("key1");
        createStreamInfoCoordinatorState("key2");
        createStreamInfoCoordinatorState("key3");
        createStreamInfoCoordinatorState("key4");
        createMigrationState();

        final List<CoordinatorState> stateList = doaUnderTest.listCoordinatorState();

        /* Verify **/
        Assertions.assertEquals(5, stateList.size());
        stateList.forEach(state -> {
            final String keyValue = state.getKey();
            if ("Migration3.0".equals(keyValue)) {
                Assertions.assertTrue(state instanceof MigrationState);
                final MigrationState migrationState = (MigrationState) state;
                Assertions.assertEquals(
                        ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, migrationState.getClientVersion());
                return;
            }
            Assertions.assertTrue(state instanceof StreamInfo);
            final StreamInfo streamInfo = (StreamInfo) state;
            Assertions.assertEquals(keyValue + "Id", streamInfo.getStreamId());
        });
    }

    @Test
    public void testListCoordinatorStateByEntityType() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = createDAO(getCoordinatorStateConfig("testListCoordinatorState"));
        doaUnderTest.initialize();

        /* Test step - create a few coordinatorState items with different schema and invoke the test to list items */
        createStreamInfoCoordinatorState("key1");
        createStreamInfoCoordinatorState("key2");
        createStreamInfoCoordinatorState("key3");
        createStreamInfoCoordinatorState("key4");
        createStreamInfoCoordinatorState("key5");
        createMigrationState();

        final List<CoordinatorState> stateList =
                doaUnderTest.listCoordinatorStateByEntityType(EntityType.CoordinatorStateType.STREAM_INFO);

        /* Verify **/
        Assertions.assertEquals(5, stateList.size());
        stateList.forEach(state -> {
            final String keyValue = state.getKey();
            Assertions.assertTrue(state instanceof StreamInfo);
            final StreamInfo streamInfo = (StreamInfo) state;
            Assertions.assertEquals(keyValue + "Id", streamInfo.getStreamId());
        });
    }

    @Test
    public void testCreateCoordinatorState_ItemExists() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest =
                createDAO(dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
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
    public void testCreateCoordinatorState_ItemNotExists() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest =
                createDAO(dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();

        /* Test step - create a few coordinatorState items with different schema and invoke the test to list items */
        createStreamInfoCoordinatorState("key1");
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
    public void testUpdateCoordinatorStateWithExpectation_Success() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest =
                createDAO(dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();
        MigrationState state = createMigrationState();

        /* Test step - update the state */
        MigrationState updatedSate =
                state.copy().update(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK, DUMMY_WORKER_ID);

        final boolean updated = doaUnderTest.updateCoordinatorStateWithExpectation(
                updatedSate, state.getDynamoClientVersionExpectation());

        /* Verify - update succeeded **/
        Assertions.assertTrue(updated);
    }

    @Test
    public void testUpdateCoordinatorStateWithExpectation_ConditionFailed() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest =
                createDAO(dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
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
                        .tableName(LEASE_TABLE_NAME)
                        .key(new HashMap<String, AttributeValue>() {
                            {
                                put(DynamoDBLeaseSerializer.LEASE_KEY_KEY, AttributeValue.fromS("Migration3.0"));
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
        Assertions.assertEquals(2, response.item().get("h").l().size());
        Assertions.assertEquals(
                state.getClientVersion().name(),
                response.item().get("h").l().get(0).m().get("cv").s());
        Assertions.assertEquals(
                state.getModifiedBy(),
                response.item().get("h").l().get(0).m().get("mb").s());
        Assertions.assertEquals(
                String.valueOf(state.getModifiedTimestamp()),
                response.item().get("h").l().get(0).m().get("mts").n());
        Assertions.assertEquals(
                ClientVersion.CLIENT_VERSION_INIT.name(),
                response.item().get("h").l().get(1).m().get("cv").s());

        log.info("Response {}", response);
    }

    @Test
    public void testUpdateCoordinatorStateWithExpectation_NonExistentKey() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest =
                createDAO(dynamoDbAsyncClient, getCoordinatorStateConfig("testCreatingLeaderAndMigrationKey"));
        doaUnderTest.initialize();

        /* Test step - update with new state object */
        final MigrationState updatedState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_2X, WORKER_ID);

        boolean updated = doaUnderTest.updateCoordinatorStateWithExpectation(updatedState, null);

        /* Verify - update failed **/
        Assertions.assertFalse(updated);
    }

    @Test
    public void testDeleteCoordinatorState_ExistingKey() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = createDAO(getCoordinatorStateConfig("testDeleteCoordinatorState"));
        doaUnderTest.initialize();
        createStreamInfoCoordinatorState("key1");

        /* Verify item exists before deletion */
        CoordinatorState stateBeforeDelete = doaUnderTest.getCoordinatorState("key1");
        Assertions.assertNotNull(stateBeforeDelete);

        /* Test step - delete the state */
        boolean deleted = doaUnderTest.deleteCoordinatorState("key1");

        /* Verify - delete succeeded and item no longer exists */
        Assertions.assertTrue(deleted);
        CoordinatorState stateAfterDelete = doaUnderTest.getCoordinatorState("key1");
        Assertions.assertNull(stateAfterDelete);
    }

    @Test
    public void testDeleteCoordinatorState_NonExistentKey() throws Exception {
        /* Test setup - create class under test and initialize **/
        final CoordinatorStateDAO doaUnderTest = createDAO(getCoordinatorStateConfig("testDeleteCoordinatorState"));
        doaUnderTest.initialize();

        /* Verify item doesn't exist before deletion */
        CoordinatorState stateBeforeDelete = doaUnderTest.getCoordinatorState("nonExistentKey");
        Assertions.assertNull(stateBeforeDelete);

        /* Test step - delete the non-existent state */
        boolean deleted = doaUnderTest.deleteCoordinatorState("nonExistentKey");

        /* Verify - delete operation still returns true even though item didn't exist */
        Assertions.assertTrue(deleted);
        CoordinatorState stateAfterDelete = doaUnderTest.getCoordinatorState("nonExistentKey");
        Assertions.assertNull(stateAfterDelete);
    }

    @Test
    public void testDeleteCoordinatorState_MultipleItems() throws Exception {
        final CoordinatorStateDAO doaUnderTest = createDAO(getCoordinatorStateConfig("testDeleteCoordinatorState"));
        doaUnderTest.initialize();
        createStreamInfoCoordinatorState("key1");
        createStreamInfoCoordinatorState("key2");
        createStreamInfoCoordinatorState("key3");

        /* Verify items exist before deletion */
        List<CoordinatorState> statesBeforeDelete = doaUnderTest.listCoordinatorState();
        Assertions.assertEquals(3, statesBeforeDelete.size());

        /* Test step - delete one state */
        boolean deleted = doaUnderTest.deleteCoordinatorState("key2");

        /* Verify - delete succeeded and only the specified item was deleted */
        Assertions.assertTrue(deleted);
        List<CoordinatorState> statesAfterDelete = doaUnderTest.listCoordinatorState();
        Assertions.assertEquals(2, statesAfterDelete.size());

        boolean foundKey1 = false;
        boolean foundKey3 = false;

        for (CoordinatorState state : statesAfterDelete) {
            if ("key1".equals(state.getKey())) {
                foundKey1 = true;
            } else if ("key3".equals(state.getKey())) {
                foundKey3 = true;
            } else if ("key2".equals(state.getKey())) {
                Assertions.fail("key2 should have been deleted");
            }
        }

        Assertions.assertTrue(foundKey1, "key1 should still exist");
        Assertions.assertTrue(foundKey3, "key3 should still exist");
    }

    private CoordinatorStateTableConfig getCoordinatorStateConfig(final String applicationName) {
        return getCoordinatorStateConfig(applicationName, BillingMode.PAY_PER_REQUEST, null, null);
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

        return c.coordinatorStateTableConfig();
    }

    private void createStreamInfoCoordinatorState(final String keyValue) {
        StreamInfo streamInfo = new StreamInfo(keyValue, keyValue + "Id");
        dynamoDbAsyncClient
                .putItem(PutItemRequest.builder()
                        .tableName(LEASE_TABLE_NAME)
                        .item(new HashMap<String, AttributeValue>() {
                            {
                                put(DynamoDBLeaseSerializer.LEASE_KEY_KEY, AttributeValue.fromS(keyValue));
                                putAll(streamInfo.serialize());
                            }
                        })
                        .build())
                .join();
    }

    private void createCoordinatorStateWithEntityType(final String keyValue, String entityType) {
        dynamoDbAsyncClient
                .putItem(PutItemRequest.builder()
                        .tableName(LEASE_TABLE_NAME)
                        .item(new HashMap<String, AttributeValue>() {
                            {
                                put(DynamoDBLeaseSerializer.LEASE_KEY_KEY, AttributeValue.fromS(keyValue));
                                put(keyValue + "-StrAttr", AttributeValue.fromS(keyValue + "_strVal"));
                                put(keyValue + "-IntAttr", AttributeValue.fromN("100"));
                                put("entityType", AttributeValue.fromS(entityType));
                            }
                        })
                        .build())
                .join();
    }

    private MigrationState createMigrationState() {
        final HashMap<String, AttributeValue> item = new HashMap<String, AttributeValue>() {
            {
                put(DynamoDBLeaseSerializer.LEASE_KEY_KEY, AttributeValue.fromS("Migration3.0"));
                putAll(new MigrationState(DUMMY_WORKER_ID)
                        .update(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, DUMMY_WORKER_ID)
                        .serialize());
            }
        };

        dynamoDbAsyncClient
                .putItem(PutItemRequest.builder()
                        .tableName(LEASE_TABLE_NAME)
                        .item(item)
                        .build())
                .join();

        item.remove("key");

        return MigrationState.deserialize("Migration3.0", item);
    }

    private CoordinatorStateDAO createDAO(final CoordinatorStateTableConfig config) throws Exception {
        return createDAO(dynamoDbAsyncClient, config);
    }

    private CoordinatorStateDAO createDAO(final DynamoDbAsyncClient client, final CoordinatorStateTableConfig config)
            throws Exception {
        leaseRefresher.createLeaseTableIfNotExists();

        final TableMigrationStatusProvider provider = mock(TableMigrationStatusProvider.class);
        when(provider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        return new CoordinatorStateDAO(client, config, LEASE_TABLE_NAME, provider);
    }
}
