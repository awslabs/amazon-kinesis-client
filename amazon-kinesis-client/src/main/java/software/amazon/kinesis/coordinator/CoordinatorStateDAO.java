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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.coordinator.migration.MigrationState;
import software.amazon.kinesis.leases.DynamoUtils;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.utils.DdbUtil;

import static java.util.Objects.nonNull;
import static software.amazon.kinesis.common.FutureUtils.unwrappingFuture;
import static software.amazon.kinesis.coordinator.CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME;

/**
 * Data Access Object to abstract accessing {@link CoordinatorState} from
 * the CoordinatorState DDB table.
 */
@Slf4j
public class CoordinatorStateDAO {
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final DynamoDbClient dynamoDbSyncClient;

    private final CoordinatorStateTableConfig config;

    public CoordinatorStateDAO(
            final DynamoDbAsyncClient dynamoDbAsyncClient, final CoordinatorStateTableConfig config) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.config = config;
        this.dynamoDbSyncClient = createDelegateClient();
    }

    public void initialize() throws DependencyException {
        createTableIfNotExists();
    }

    private DynamoDbClient createDelegateClient() {
        return new DynamoDbAsyncToSyncClientAdapter(dynamoDbAsyncClient);
    }

    public AmazonDynamoDBLockClientOptionsBuilder getDDBLockClientOptionsBuilder() {
        return AmazonDynamoDBLockClientOptions.builder(dynamoDbSyncClient, config.tableName())
                .withPartitionKeyName(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME);
    }

    /**
     * List all the {@link CoordinatorState} from the DDB table synchronously
     *
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if ddb table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     *
     * @return list of state
     */
    public List<CoordinatorState> listCoordinatorState()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        log.debug("Listing coordinatorState");

        final ScanRequest request =
                ScanRequest.builder().tableName(config.tableName()).build();

        try {
            ScanResponse response = FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.scan(request));
            final List<CoordinatorState> stateList = new ArrayList<>();
            while (Objects.nonNull(response)) {
                log.debug("Scan response {}", response);

                response.items().stream().map(this::fromDynamoRecord).forEach(stateList::add);
                if (!CollectionUtils.isNullOrEmpty(response.lastEvaluatedKey())) {
                    final ScanRequest continuationRequest = request.toBuilder()
                            .exclusiveStartKey(response.lastEvaluatedKey())
                            .build();
                    log.debug("Scan request {}", continuationRequest);
                    response = FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.scan(continuationRequest));
                } else {
                    log.debug("Scan finished");
                    response = null;
                }
            }
            return stateList;
        } catch (final ProvisionedThroughputExceededException e) {
            log.warn(
                    "Provisioned throughput on {} has exceeded. It is recommended to increase the IOPs"
                            + " on the table.",
                    config.tableName());
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(
                    String.format("Cannot list coordinatorState, because table %s does not exist", config.tableName()));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }
    }

    /**
     * Create a new {@link CoordinatorState} if it does not exist.
     * @param state the state to create
     * @return true if state was created, false if it already exists
     *
     * @throws DependencyException if DynamoDB put fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB put fails due to lack of capacity
     */
    public boolean createCoordinatorStateIfNotExists(final CoordinatorState state)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Creating coordinatorState {}", state);

        final PutItemRequest request = PutItemRequest.builder()
                .tableName(config.tableName())
                .item(toDynamoRecord(state))
                .expected(getDynamoNonExistentExpectation())
                .build();

        try {
            FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.putItem(request));
        } catch (final ConditionalCheckFailedException e) {
            log.info("Not creating coordinator state because the key already exists");
            return false;
        } catch (final ProvisionedThroughputExceededException e) {
            log.warn(
                    "Provisioned throughput on {} has exceeded. It is recommended to increase the IOPs"
                            + " on the table.",
                    config.tableName());
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(String.format(
                    "Cannot create coordinatorState %s, because table %s does not exist", state, config.tableName()));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }

        log.info("Created CoordinatorState: {}", state);
        return true;
    }

    /**
     * @param key Get the CoordinatorState for this key
     *
     * @throws InvalidStateException if ddb table does not exist
     * @throws ProvisionedThroughputException if DynamoDB get fails due to lack of capacity
     * @throws DependencyException if DynamoDB get fails in an unexpected way
     *
     * @return state for the specified key, or null if one doesn't exist
     */
    public CoordinatorState getCoordinatorState(@NonNull final String key)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Getting coordinatorState with key {}", key);

        final GetItemRequest request = GetItemRequest.builder()
                .tableName(config.tableName())
                .key(getCoordinatorStateKey(key))
                .consistentRead(true)
                .build();

        try {
            final GetItemResponse result = FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.getItem(request));

            final Map<String, AttributeValue> dynamoRecord = result.item();
            if (CollectionUtils.isNullOrEmpty(dynamoRecord)) {
                log.debug("No coordinatorState found with key {}, returning null.", key);
                return null;
            }
            return fromDynamoRecord(dynamoRecord);
        } catch (final ProvisionedThroughputExceededException e) {
            log.warn(
                    "Provisioned throughput on {} has exceeded. It is recommended to increase the IOPs"
                            + " on the table.",
                    config.tableName());
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(String.format(
                    "Cannot get coordinatorState for key %s, because table %s does not exist",
                    key, config.tableName()));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }
    }

    /**
     * Update fields of the given coordinator state in DynamoDB. Conditional on the provided expectation.
     *
     * @return true if update succeeded, false otherwise when expectations are not met
     *
     * @throws InvalidStateException if table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    public boolean updateCoordinatorStateWithExpectation(
            @NonNull final CoordinatorState state, final Map<String, ExpectedAttributeValue> expectations)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final Map<String, ExpectedAttributeValue> expectationMap = getDynamoExistentExpectation(state.getKey());
        expectationMap.putAll(MapUtils.emptyIfNull(expectations));

        final Map<String, AttributeValueUpdate> updateMap = getDynamoCoordinatorStateUpdate(state);

        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(config.tableName())
                .key(getCoordinatorStateKey(state.getKey()))
                .expected(expectationMap)
                .attributeUpdates(updateMap)
                .build();

        try {
            FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.updateItem(request));
        } catch (final ConditionalCheckFailedException e) {
            log.debug("CoordinatorState update {} failed because conditions were not met", state);
            return false;
        } catch (final ProvisionedThroughputExceededException e) {
            log.warn(
                    "Provisioned throughput on {} has exceeded. It is recommended to increase the IOPs"
                            + " on the table.",
                    config.tableName());
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(String.format(
                    "Cannot update coordinatorState for key %s, because table %s does not exist",
                    state.getKey(), config.tableName()));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }

        log.info("Coordinator state updated {}", state);
        return true;
    }

    private void createTableIfNotExists() throws DependencyException {
        TableDescription tableDescription = getTableDescription();
        if (tableDescription == null) {
            final CreateTableResponse response = unwrappingFuture(() -> dynamoDbAsyncClient.createTable(getRequest()));
            tableDescription = response.tableDescription();
            log.info("DDB Table: {} created", config.tableName());
        } else {
            log.info("Skipping DDB table {} creation as it already exists", config.tableName());
        }

        if (tableDescription.tableStatus() != TableStatus.ACTIVE) {
            log.info("Waiting for DDB Table: {} to become active", config.tableName());
            try (final DynamoDbAsyncWaiter waiter = dynamoDbAsyncClient.waiter()) {
                final WaiterResponse<DescribeTableResponse> response =
                        unwrappingFuture(() -> waiter.waitUntilTableExists(
                                r -> r.tableName(config.tableName()), o -> o.waitTimeout(Duration.ofMinutes(10))));
                response.matched()
                        .response()
                        .orElseThrow(() -> new DependencyException(new IllegalStateException(
                                "Creating CoordinatorState table timed out",
                                response.matched().exception().orElse(null))));
            }
            unwrappingFuture(() -> DdbUtil.pitrEnabler(config, dynamoDbAsyncClient));
        }
    }

    private CreateTableRequest getRequest() {
        final CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder()
                .tableName(config.tableName())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME)
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .deletionProtectionEnabled(config.deletionProtectionEnabled());

        if (nonNull(config.tags()) && !config.tags().isEmpty()) {
            requestBuilder.tags(config.tags());
        }

        switch (config.billingMode()) {
            case PAY_PER_REQUEST:
                requestBuilder.billingMode(BillingMode.PAY_PER_REQUEST);
                break;
            case PROVISIONED:
                requestBuilder.billingMode(BillingMode.PROVISIONED);

                final ProvisionedThroughput throughput = ProvisionedThroughput.builder()
                        .readCapacityUnits(config.readCapacity())
                        .writeCapacityUnits(config.writeCapacity())
                        .build();
                requestBuilder.provisionedThroughput(throughput);
                break;
        }
        return requestBuilder.build();
    }

    private Map<String, AttributeValue> getCoordinatorStateKey(@NonNull final String key) {
        return Collections.singletonMap(
                COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME, DynamoUtils.createAttributeValue(key));
    }

    private CoordinatorState fromDynamoRecord(final Map<String, AttributeValue> dynamoRecord) {
        final HashMap<String, AttributeValue> attributes = new HashMap<>(dynamoRecord);
        final String keyValue =
                DynamoUtils.safeGetString(attributes.remove(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME));

        final MigrationState migrationState = MigrationState.deserialize(keyValue, attributes);
        if (migrationState != null) {
            log.debug("Retrieved MigrationState {}", migrationState);
            return migrationState;
        }

        final CoordinatorState c =
                CoordinatorState.builder().key(keyValue).attributes(attributes).build();
        log.debug("Retrieved coordinatorState {}", c);

        return c;
    }

    private Map<String, AttributeValue> toDynamoRecord(final CoordinatorState state) {
        final Map<String, AttributeValue> result = new HashMap<>();
        result.put(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME, DynamoUtils.createAttributeValue(state.getKey()));
        if (state instanceof MigrationState) {
            result.putAll(((MigrationState) state).serialize());
        }
        if (!CollectionUtils.isNullOrEmpty(state.getAttributes())) {
            result.putAll(state.getAttributes());
        }
        return result;
    }

    private Map<String, ExpectedAttributeValue> getDynamoNonExistentExpectation() {
        final Map<String, ExpectedAttributeValue> result = new HashMap<>();

        final ExpectedAttributeValue expectedAV =
                ExpectedAttributeValue.builder().exists(false).build();
        result.put(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME, expectedAV);

        return result;
    }

    private Map<String, ExpectedAttributeValue> getDynamoExistentExpectation(final String keyValue) {
        final Map<String, ExpectedAttributeValue> result = new HashMap<>();

        final ExpectedAttributeValue expectedAV = ExpectedAttributeValue.builder()
                .value(AttributeValue.fromS(keyValue))
                .build();
        result.put(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME, expectedAV);

        return result;
    }

    private Map<String, AttributeValueUpdate> getDynamoCoordinatorStateUpdate(final CoordinatorState state) {
        final HashMap<String, AttributeValueUpdate> updates = new HashMap<>();
        if (state instanceof MigrationState) {
            updates.putAll(((MigrationState) state).getDynamoUpdate());
        }
        state.getAttributes()
                .forEach((attribute, value) -> updates.put(
                        attribute,
                        AttributeValueUpdate.builder()
                                .value(value)
                                .action(AttributeAction.PUT)
                                .build()));
        return updates;
    }

    private TableDescription getTableDescription() {
        try {
            final DescribeTableResponse response = unwrappingFuture(() -> dynamoDbAsyncClient.describeTable(
                    DescribeTableRequest.builder().tableName(config.tableName()).build()));
            return response.table();
        } catch (final ResourceNotFoundException e) {
            return null;
        }
    }
}
