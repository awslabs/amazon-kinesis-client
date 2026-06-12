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
package software.amazon.kinesis.coordinator.delegate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.DynamoDbAsyncToSyncClientAdapter;
import software.amazon.kinesis.coordinator.migration.MigrationState;
import software.amazon.kinesis.coordinator.migration.TableMigrationState;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfo;
import software.amazon.kinesis.leases.DynamoUtils;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * Data Access Object to abstract accessing {@link CoordinatorState} from
 * the DDB table it is stored in.
 */
@Slf4j
@KinesisClientInternalApi
public abstract class CoordinatorStateDAODelegate {
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final DynamoDbClient dynamoDbSyncClient;

    private final String tableName;
    private final String partitionKeyAttributeName;

    private static final String ENTITY_TYPE_ATTRIBUTE_NAME = "entityType";
    private static final String DDB_ENTITY_TYPE = ":entityType";

    /**
     * Registry of deserializers keyed by the entityType DDB attribute value.
     * Used for O(1) lookup during deserialization.
     */
    private final Map<String, BiFunction<String, Map<String, AttributeValue>, CoordinatorState>> deserializers =
            new HashMap<>();

    public CoordinatorStateDAODelegate(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            final String tableName,
            final String partitionKeyAttributeName) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.tableName = tableName;
        this.partitionKeyAttributeName = partitionKeyAttributeName;
        this.dynamoDbSyncClient = createDelegateClient();
        registerDeserializers();
    }

    public String getTableName() {
        return tableName;
    }

    public String getPartitionKeyAttributeName() {
        return partitionKeyAttributeName;
    }

    private void registerDeserializers() {
        deserializers.put(
                EntityType.STREAM_INFO.getDdbValue(), (key, attributes) -> StreamInfo.deserialize(key, attributes));
        deserializers.put(
                EntityType.CLIENT_VERSION_MIGRATION.getDdbValue(),
                (key, attributes) -> MigrationState.deserialize(key, new HashMap<>(attributes)));
        deserializers.put(
                EntityType.TABLE_MIGRATION.getDdbValue(),
                (key, attributes) -> TableMigrationState.deserialize(key, new HashMap<>(attributes)));
    }

    /**
     * Resolve the EntityType from a DDB record. Uses the entityType attribute if present,
     * otherwise infers from well-known partition key values.
     */
    private EntityType resolveEntityType(final String key, final Map<String, AttributeValue> attributes) {
        final AttributeValue entityTypeAttr = attributes.get(ENTITY_TYPE_ATTRIBUTE_NAME);
        if (entityTypeAttr != null && entityTypeAttr.s() != null) {
            final EntityType resolved = EntityType.fromDdbValue(entityTypeAttr.s());
            if (resolved != null) {
                return resolved;
            }
        }
        // Infer from well-known keys for legacy records without entityType attribute
        if (MigrationState.MIGRATION_HASH_KEY.equals(key)) {
            return EntityType.CLIENT_VERSION_MIGRATION;
        }
        if (TableMigrationState.TABLE_MIGRATION_HASH_KEY.equals(key)) {
            return EntityType.TABLE_MIGRATION;
        }
        return null;
    }

    public abstract void initialize() throws DependencyException;

    private DynamoDbClient createDelegateClient() {
        return new DynamoDbAsyncToSyncClientAdapter(dynamoDbAsyncClient);
    }

    public AmazonDynamoDBLockClientOptionsBuilder getDDBLockClientOptionsBuilder() {
        return AmazonDynamoDBLockClientOptions.builder(dynamoDbSyncClient, tableName)
                .withPartitionKeyName(partitionKeyAttributeName);
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
                ScanRequest.builder().tableName(tableName).consistentRead(true).build();

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
                    tableName);
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(
                    String.format("Cannot list coordinatorState, because table %s does not exist", tableName));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }
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
    public List<CoordinatorState> listCoordinatorStateByEntityType(EntityType.CoordinatorStateType entityType)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        log.debug("Listing coordinatorState");

        final Map<String, AttributeValue> expressionAttributeValues = ImmutableMap.of(
                DDB_ENTITY_TYPE,
                AttributeValue.builder().s(entityType.getDdbValue()).build());

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .filterExpression(ENTITY_TYPE_ATTRIBUTE_NAME + " = " + DDB_ENTITY_TYPE)
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        try {
            ScanResponse response = FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.scan(scanRequest));
            final List<CoordinatorState> stateList = new ArrayList<>();

            while (Objects.nonNull(response)) {
                log.debug("Scan response {}", response);
                response.items().stream().map(this::fromDynamoRecord).forEach(stateList::add);

                if (!CollectionUtils.isNullOrEmpty(response.lastEvaluatedKey())) {
                    final ScanRequest continuationRequest = scanRequest.toBuilder()
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
                    "Provisioned throughput on {} has exceeded. It is recommended to increase the IOPs on the table.",
                    tableName);
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(
                    String.format("Cannot list coordinatorState, because table %s does not exist", tableName));
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
                .tableName(tableName)
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
                    tableName);
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(String.format(
                    "Cannot create coordinatorState %s, because table %s does not exist", state, tableName));
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
                .tableName(tableName)
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
                    tableName);
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(String.format(
                    "Cannot get coordinatorState for key %s, because table %s does not exist", key, tableName));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }
    }

    /**
     * Create a new {@link CoordinatorState} if it does not exist.
     * @param key the key to delete
     * @return true if state was deleted, false if you cannot be deleted
     *
     * @throws DependencyException if DynamoDB delete fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB delete fails due to lack of capacity
     */
    public boolean deleteCoordinatorState(@NonNull final String key)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        log.debug("Deleting item with key {}", key);
        final DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(getCoordinatorStateKey(key))
                .build();
        try {
            FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.deleteItem(request));
        } catch (final ProvisionedThroughputExceededException e) {
            log.warn(
                    "Provisioned throughput on {} has exceeded. It is recommended to increase the IOPs"
                            + " on the table.",
                    tableName);
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(String.format(
                    "Cannot delete coordinatorState for key %s, because table %s does not exist", key, tableName));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }
        log.debug("Coordinator state deleted {}", key);
        return true;
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
                .tableName(tableName)
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
                    tableName);
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(String.format(
                    "Cannot update coordinatorState for key %s, because table %s does not exist",
                    state.getKey(), tableName));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }

        log.info("Coordinator state updated {}", state);
        return true;
    }

    private Map<String, AttributeValue> getCoordinatorStateKey(@NonNull final String key) {
        return Collections.singletonMap(partitionKeyAttributeName, DynamoUtils.createAttributeValue(key));
    }

    public CoordinatorState fromDynamoRecord(final Map<String, AttributeValue> dynamoRecord) {
        final HashMap<String, AttributeValue> attributes = new HashMap<>(dynamoRecord);
        // log.info("fromDynamoRecord {} attributes {}", dynamoRecord, attributes);

        final String keyValue = DynamoUtils.safeGetString(attributes.remove(partitionKeyAttributeName));
        // log.info("fromDynamoRecord {} attributes {}", keyValue, attributes);

        final EntityType entityType = resolveEntityType(keyValue, attributes);
        if (entityType != null) {
            final BiFunction<String, Map<String, AttributeValue>, CoordinatorState> deserializer =
                    deserializers.get(entityType.getDdbValue());
            if (deserializer != null) {
                final CoordinatorState result = deserializer.apply(keyValue, attributes);
                if (result != null) {
                    log.debug(
                            "Deserialized {} for entityType {}",
                            result.getClass().getSimpleName(),
                            entityType);
                    return result;
                }
            }
        }

        // Fallback: generic CoordinatorState for unknown entity types (e.g., Leader lock)
        final CoordinatorState c =
                CoordinatorState.builder().key(keyValue).attributes(attributes).build();
        log.info("Retrieved generic coordinatorState {} attributes {}", c, attributes);
        return c;
    }

    private Map<String, AttributeValue> toDynamoRecord(final CoordinatorState state) {
        final Map<String, AttributeValue> result = new HashMap<>();
        result.put(partitionKeyAttributeName, DynamoUtils.createAttributeValue(state.getKey()));
        result.putAll(state.serialize());
        return result;
    }

    /**
     * Converts a {@link CoordinatorState} to its full DDB item representation including the
     * partition key. Public variant of {@link #toDynamoRecord} for use in transactional writes
     * built externally (e.g., conditional Puts with custom condition expressions).
     */
    public Map<String, AttributeValue> toTransactRecord(final CoordinatorState state) {
        return toDynamoRecord(state);
    }

    private Map<String, ExpectedAttributeValue> getDynamoNonExistentExpectation() {
        final Map<String, ExpectedAttributeValue> result = new HashMap<>();

        final ExpectedAttributeValue expectedAV =
                ExpectedAttributeValue.builder().exists(false).build();
        result.put(partitionKeyAttributeName, expectedAV);

        return result;
    }

    private Map<String, ExpectedAttributeValue> getDynamoExistentExpectation(final String keyValue) {
        final Map<String, ExpectedAttributeValue> result = new HashMap<>();

        final ExpectedAttributeValue expectedAV = ExpectedAttributeValue.builder()
                .value(AttributeValue.fromS(keyValue))
                .build();
        result.put(partitionKeyAttributeName, expectedAV);

        return result;
    }

    private Map<String, AttributeValueUpdate> getDynamoCoordinatorStateUpdate(final CoordinatorState state) {
        return state.getDynamoUpdate();
    }

    // ==================== Transactional Helpers ====================

    /**
     * Creates a {@link TransactWriteItem} that performs a conditional Put (item must not exist)
     * for the given coordinator state into this delegate's table.
     *
     * @param state the coordinator state to put
     * @return a TransactWriteItem for use in a DynamoDB TransactWriteItems call
     */
    public TransactWriteItem createTransactPut(final CoordinatorState state) {
        final Put put = Put.builder()
                .tableName(tableName)
                .item(toDynamoRecord(state))
                .conditionExpression("attribute_not_exists(" + partitionKeyAttributeName + ")")
                .build();
        return TransactWriteItem.builder().put(put).build();
    }

    /**
     * Creates a {@link TransactWriteItem} that deletes the entry with the given key
     * from this delegate's table.
     *
     * @param key the partition key of the entry to delete
     * @return a TransactWriteItem for use in a DynamoDB TransactWriteItems call
     */
    public TransactWriteItem createTransactDelete(@NonNull final String key) {
        final Delete delete = Delete.builder()
                .tableName(tableName)
                .key(getCoordinatorStateKey(key))
                .build();
        return TransactWriteItem.builder().delete(delete).build();
    }
}
