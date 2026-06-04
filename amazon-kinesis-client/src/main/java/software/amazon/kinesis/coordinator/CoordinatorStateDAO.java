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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
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
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
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
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.coordinator.migration.MigrationState;
import software.amazon.kinesis.coordinator.migration.TableMigrationMachine;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfo;
import software.amazon.kinesis.leases.DynamoUtils;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.utils.DdbUtil;

import static java.util.Objects.nonNull;
import static software.amazon.kinesis.common.FutureUtils.unwrappingFuture;
import static software.amazon.kinesis.coordinator.CoordinatorState.COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME;
import static software.amazon.kinesis.coordinator.CoordinatorState.LEADER_HASH_KEY;
import static software.amazon.kinesis.leader.DynamoDBLockBasedLeaderDecider.STEADY_SINCE_ATTRIBUTE_NAME;
import static software.amazon.kinesis.leader.DynamoDBLockBasedLeaderDecider.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME;
import static software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer.LEASE_KEY_KEY;

/**
 * Data Access Object to abstract accessing {@link CoordinatorState} from
 * the CoordinatorState DDB table.
 */
@Slf4j
@KinesisClientInternalApi
public class CoordinatorStateDAO {
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final DynamoDbClient dynamoDbSyncClient;

    /** the config object stores the DdbTableConfig for the coordinator table, whether we're using it or the lease table */
    private final CoordinatorStateTableConfig config;

    private String tableName;
    private String partitionKeyName;
    private LeaseManagementConfig leaseManagementConfig;

    @Getter
    private Map<String, AttributeValue> leaderLockItemSnapshot;

    @Getter
    private boolean usingLeaseTable;

    private CoordinatorStateDAO coordinatorTableDAO;

    /** entityType field may not exist for all coordinator states in coordinator table, but should exist in lease table */
    private static final String ENTITY_TYPE = "entityType";

    private static final String DDB_ENTITY_TYPE = ":entityType";

    public CoordinatorStateDAO(
            final DynamoDbAsyncClient dynamoDbAsyncClient, final CoordinatorStateTableConfig config) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.config = config;
        this.dynamoDbSyncClient = createDelegateClient();
        setUsingLeaseTable(false);
    }

    // TODO: create constructor that just takes LeaseManagementConfig and builds CoordinatorStateTableConfig on its own?
    public CoordinatorStateDAO(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            final CoordinatorStateTableConfig config,
            final LeaseManagementConfig leaseManagementConfig) {
        this(dynamoDbAsyncClient, config);
        this.leaseManagementConfig = leaseManagementConfig;
        this.coordinatorTableDAO = new CoordinatorStateDAO(dynamoDbAsyncClient, config);

        // try to find leader lock item in lease table; if not exists, fallback to coordinator table
        if (getLeaderLockSnapshot(true) || getLeaderLockSnapshot(false)) {
            respondToTableMigrationStatus(getTableMigrationStatus(leaderLockItemSnapshot));
        } else {
            // if leader lock item can't be found in either table, we must be upgrading from v2
            setUsingLeaseTable(true);
        }
    }

    public void initialize() throws DependencyException {
        if (!usingLeaseTable) {
            // don't create lease table, let existing lease management lifecycle handle; only create coordinator table
            createTableIfNotExists();
        }
    }

    private DynamoDbClient createDelegateClient() {
        return new DynamoDbAsyncToSyncClientAdapter(dynamoDbAsyncClient);
    }

    private void setUsingLeaseTable(boolean using) {
        usingLeaseTable = using;
        tableName = using ? leaseManagementConfig.tableName() : config.tableName();
        partitionKeyName = using ? LEASE_KEY_KEY : COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME;
    }

    private boolean getLeaderLockSnapshot(boolean usingLeaseTable) {
        setUsingLeaseTable(usingLeaseTable);
        return getLeaderLockSnapshot();
    }

    private boolean getLeaderLockSnapshot() {
        CoordinatorState leaderLock = tryGetCoordinatorState(CoordinatorState.LEADER_HASH_KEY, 3);
        return leaderLock != null && (leaderLockItemSnapshot = leaderLock.getAttributes()) != null;
    }

    public void respondToTableMigrationStatus(String tableMigrationStatus) {
        setUsingLeaseTable(false); // default setting

        if (tableMigrationStatus == null) {
            return;
        }
        switch (tableMigrationStatus) {
            case "DEPLOYED": {
                if (!leaseManagementConfig.migrateAllEntityTypesToLeaseTable()) {
                    // must wait for config value to signal second-phase deployment
                    return;
                }
                // should be on second phase deployment based on config -> start table migration now that it's safe
                updateTableMigrationStatus(TableMigrationMachine.States.PENDING);
                // fall through because the new state will be PENDING
            }
            case "PENDING": {
                if (!leaseManagementConfig.migrateAllEntityTypesToLeaseTable()) {
                    // must wait for config value to signal we are part of second-phase deployment
                    return;
                }
                // sync coordinator states to coordinator table and fall through to use lease table
                MutationTracker.createInstanceIfNull(dynamoDbAsyncClient, config);
            }
            case "COMPLETE": {
                setUsingLeaseTable(true);
            }
        }
    }

    public static String getTableMigrationStatus(Map<String, AttributeValue> attributes) {
        return attributes == null
                ? null
                : DynamoUtils.safeGetString(
                        attributes.get(TableMigrationMachine.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME));
    }

    public AmazonDynamoDBLockClientOptionsBuilder getDDBLockClientOptionsBuilder() {
        return getDDBLockClientOptionsBuilder(usingLeaseTable);
    }

    public AmazonDynamoDBLockClientOptionsBuilder getDDBLockClientOptionsBuilder(boolean usingLeaseTable) {
        String tableName = config.tableName();
        String partitionKeyName = COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME;

        if (usingLeaseTable) {
            tableName = leaseManagementConfig.tableName();
            partitionKeyName = LEASE_KEY_KEY;
        }

        return AmazonDynamoDBLockClientOptions.builder(dynamoDbSyncClient, tableName)
                .withPartitionKeyName(partitionKeyName);
    }

    static class MutationTracker {
        @Getter
        private static MutationTracker instance;

        @Setter
        private boolean scanning = true; // wait until first scan completes before first sync

        private final CoordinatorStateDAO coordinatorTableDAO;
        private final Map<String, Long> lastSeenTimestamps = new HashMap<>();
        private final Set<String> justScanned = new HashSet<>();
        private final EnumMap<Mutations, Set<String>> mutations = new EnumMap<>(Mutations.class);
        private final Map<String, CoordinatorState> states = new HashMap<>();

        /**
         * The different possible changes to sync from the lease table to the coordinator table.
         *  CREATE - recorded when the scanned key isn't recognized from before (could be due to leader change)
         *  UPDATE - recorded when the scanned timestamp is higher than what we have in-memory from before
         *  DELETE - recorded when a key from before wasn't scanned this time (possible to miss due to leader change)
         */
        enum Mutations {
            CREATE,
            UPDATE,
            DELETE
        }

        MutationTracker(DynamoDbAsyncClient client, CoordinatorStateTableConfig config) {
            this.coordinatorTableDAO = new CoordinatorStateDAO(client, config);

            for (Mutations mutation : Mutations.values()) {
                this.mutations.put(mutation, new HashSet<>());
            }
        }

        static MutationTracker createInstanceIfNull(DynamoDbAsyncClient client, CoordinatorStateTableConfig config) {
            return (instance = instance != null ? instance : new MutationTracker(client, config));
        }

        /**
         * Processes a single passed deserialized CoordinatorState by comparing its modified timestamp against memory.
         * @param state - the constructed CoordinatorState object
         */
        synchronized void process(CoordinatorState state) {
            // if received new state through process(), sync() must wait for caller to reset flag
            // if beginning new scan, clear scanned set from previous scan
            if (!scanning) {
                scanning = true;
                justScanned.clear();
            }

            String key = state.getKey();
            states.put(key, state);
            justScanned.add(key);

            long modifiedTimestamp = state.getModifiedTimestamp();
            Long previousTimestamp = lastSeenTimestamps.get(key);

            if (previousTimestamp == null) {
                // unrecognized key; either due to leader change or due to creation, we don't know yet
                mutations.get(Mutations.CREATE).add(key);
            } else if (modifiedTimestamp > previousTimestamp) {
                // state was modified since last seen -> queue for sync
                mutations.get(Mutations.UPDATE).add(key);
            }

            // record/update seen modified timestamp
            lastSeenTimestamps.put(key, modifiedTimestamp);
        }

        synchronized void sync() {
            if (scanning) {
                log.info("Need to wait for lease table scan to finish before syncing to coordinator table.");
                return;
            }

            // detect deletions by comparing all seen keys with the set from the latest lease table scan
            for (String seen : new HashSet<>(lastSeenTimestamps.keySet())) {
                if (!justScanned.contains(seen)) {
                    mutations.get(Mutations.DELETE).add(seen);
                    lastSeenTimestamps.remove(seen);
                }
            }

            // process the queue and perform the corresponding actions
            for (Mutations mutation : Mutations.values()) {
                final Iterator<String> iterator = mutations.get(mutation).iterator();

                while (iterator.hasNext()) {
                    final String key = iterator.next();
                    try {
                        boolean success = false;
                        switch (mutation) { // can't use cleaner "switch expression" with Java 8 target release version
                            case DELETE: {
                                success = coordinatorTableDAO.deleteCoordinatorState(key);
                                break;
                            }
                            case CREATE: {
                                success = coordinatorTableDAO.createCoordinatorStateIfNotExists(states.get(key));
                                // intentional fallthrough; state might actually exist, and we need to do update instead
                            }
                            case UPDATE: {
                                success = success
                                        || coordinatorTableDAO.updateCoordinatorStateWithExpectation(
                                                states.get(key), null);
                            }
                        }
                        if (success) {
                            iterator.remove(); // use iterator to avoid ConcurrentModificationException or reprocessing
                            states.remove(key);
                        }
                    } catch (Exception e) {
                        // most exceptions already handled in create, update, and delete methods
                        log.warn(
                                "Caught exception while trying to copy coordinator state from lease table into coordinator table: "
                                        + e);
                    }
                }
            }
        }
    }

    public static void markScanComplete() {
        MutationTracker mutationTracker = MutationTracker.getInstance();
        if (mutationTracker != null) {
            mutationTracker.setScanning(false);
        }
    }

    public static void processScannedItem(final @NonNull CoordinatorState state) {
        MutationTracker mutationTracker = MutationTracker.getInstance();
        if (mutationTracker != null) {
            mutationTracker.process(state);
        }
    }

    public static void syncCoordinatorStates() {
        MutationTracker mutationTracker = MutationTracker.getInstance();
        if (mutationTracker != null) {
            mutationTracker.sync();
        }
    }

    public CoordinatorState tryGetCoordinatorState(String key, int maxAttempts) {
        for (int waitMillisBeforeRetry = 0, retries = maxAttempts - 1;
                retries >= 0;
                retries--, waitMillisBeforeRetry = Math.max(1000, waitMillisBeforeRetry * 2)) {
            try {
                if (waitMillisBeforeRetry > 0) {
                    Thread.sleep(waitMillisBeforeRetry);
                }
                return getCoordinatorState(key);
            } catch (InterruptedException ie) {
                log.warn("Thread sleep was interrupted while waiting to retry fetching coordinator state: " + ie);
                retries++; // don't count attempt; add back to retry stock
            } catch (ResourceNotFoundException rnfe) {
                // (probably) table does not exist; exception is expected -> do not retry
                log.warn("Could not find DynamoDB resource while trying to fetch coordinator state: " + rnfe);
                return null;
            } catch (Exception e) {
                log.warn("Exception caught while trying to fetch coordinator state: " + e);
            }
        }
        log.error("Exhausted all retries while trying to read coordinator state from table " + tableName);
        return null;
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

        final ScanRequest request = ScanRequest.builder().tableName(tableName).build();

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
    public List<CoordinatorState> listCoordinatorStateByEntityType(String entityType)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        log.debug("Listing coordinatorState");

        final Map<String, AttributeValue> expressionAttributeValues = ImmutableMap.of(
                DDB_ENTITY_TYPE, AttributeValue.builder().s(entityType).build());

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .filterExpression(ENTITY_TYPE + " = " + DDB_ENTITY_TYPE)
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

    public boolean copyCoordinatorStatesToLeaseTable() {
        // should be using the lease table already at this point
        setUsingLeaseTable(true);

        try {
            for (CoordinatorState state : coordinatorTableDAO.listCoordinatorState()) {
                // go through deserialize/serialize to make sure map/object is formatted for the lease table
                // if the item exists already, we assume it's up-to-date
                createCoordinatorStateIfNotExists(fromDynamoRecord(toDynamoRecord(state)));
            }
        } catch (Exception e) {
            // try again later to get full scan or to complete all updates
            log.warn("Caught exception while trying to copy coordinator states to lease table", e);
            return false;
        }
        log.info("Copied all coordinator states from coordinator table to lease table successfully");
        return true;
    }

    public boolean updateLeaderLockAdditionalAttributes(@NonNull final Map<String, AttributeValueUpdate> updates) throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        return updateLeaderLockAdditionalAttributesWithExpectation(updates, getDynamoExistentExpectation(LEADER_HASH_KEY));
    }

    /**
     * Updates the additional attributes in the leader lock item in the instance's tableName with the new values
     * @param updates - the attributes that need to be updated and their values
     * @return whether the update succeeded
     */
    public boolean updateLeaderLockAdditionalAttributesWithExpectation(@NonNull final Map<String, AttributeValueUpdate> updates, @NonNull final Map<String, ExpectedAttributeValue> expectations)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getCoordinatorStateKey(LEADER_HASH_KEY))
                .attributeUpdates(updates)
                .build();
        try {
            FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.updateItem(request));
        } catch (final ProvisionedThroughputExceededException e) {
            log.warn(
                    "Provisioned throughput on {} has exceeded. It is recommended to increase the IOPs"
                            + " on the table.",
                    tableName);
            throw new ProvisionedThroughputException(e);
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(String.format(
                    "Cannot update leader lock additional attributes because table %s does not exist.", tableName));
        } catch (final DynamoDbException e) {
            throw new DependencyException(e);
        }
        log.info("Leader lock additional attributes updated: {}", updates);
        return true;
    }

    private Map<String, AttributeValueUpdate> getTableMigrationStatusUpdate(TableMigrationMachine.States tableMigrationStatus, long steadySinceEpoch) {
        Map<String, AttributeValueUpdate> updates = new HashMap<>();

        // build the two updates
        final AttributeValueUpdate tableMigrationStatusUpdate = AttributeValueUpdate.builder()
                .value(AttributeValue.fromS(String.valueOf(tableMigrationStatus)))
                .action(AttributeAction.PUT)
                .build();
        final AttributeValueUpdate steadySinceEpochUpdate = AttributeValueUpdate.builder()
                .value(AttributeValue.fromN(String.valueOf(steadySinceEpoch)))
                .action(AttributeAction.PUT)
                .build();

        // add the two updates to the map
        updates.put(TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME, tableMigrationStatusUpdate);
        updates.put(STEADY_SINCE_ATTRIBUTE_NAME, steadySinceEpochUpdate);

        return updates;
    }

    private Map<String, ExpectedAttributeValue> getTableMigrationStatusExpectation(TableMigrationMachine.States tableMigrationStatus, long steadySinceEpoch) {
        // start with expectation that leader lock exists
        Map<String, ExpectedAttributeValue> expectations = getDynamoExistentExpectation(LEADER_HASH_KEY);

        // build the two expectations
        final ExpectedAttributeValue tableMigrationStatusExpectation = ExpectedAttributeValue.builder()
                .value(AttributeValue.fromS(String.valueOf(tableMigrationStatus)))
                .build();
        final ExpectedAttributeValue steadySinceEpochExpectation = ExpectedAttributeValue.builder()
                .value(AttributeValue.fromN(String.valueOf(steadySinceEpoch)))
                .build();

        // add the expectations to the map
        expectations.put(TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME, tableMigrationStatusExpectation);
        expectations.put(STEADY_SINCE_ATTRIBUTE_NAME, steadySinceEpochExpectation);

        return expectations;
    }

    public boolean updateTableMigrationStatus(TableMigrationMachine.States tableMigrationStatus) {
        boolean success = false;
        try {
            success = updateLeaderLockAdditionalAttributes(getTableMigrationStatusUpdate(tableMigrationStatus, Instant.now().getEpochSecond()));
        } catch (Exception e) {
            // no need to retry; if the fields aren't updated, other workers will come online and try also
            log.warn("Caught exception while trying to update the table migration status in DynamoDB to {}", tableMigrationStatus, e);
            // success will still be false
        }
        if (!success) {
            // could be false from exception or existent expectation failure
            log.warn("Failed to update table migration status in DynamoDB to {}.", tableMigrationStatus);
            return false;
        } else {
            log.info("Updated table migration status in the leader lock item to {}.", tableMigrationStatus);
            return true;
        }
    }

    public boolean updateTableMigrationStatusWithExpectation(
            TableMigrationMachine.States oldTableMigrationStatus,
            TableMigrationMachine.States newTableMigrationStatus,
            long oldSteadySinceEpoch) {
        boolean success = false;
        try {
            success = updateLeaderLockAdditionalAttributesWithExpectation(
                    getTableMigrationStatusUpdate(newTableMigrationStatus, Instant.now().getEpochSecond()),
                    getTableMigrationStatusExpectation(oldTableMigrationStatus, oldSteadySinceEpoch));
        } catch (Exception e) {
            // no need to retry; if the fields aren't updated, other workers will come online and try also
            log.warn("Caught exception while trying to update the table migration status in DynamoDB to {}", newTableMigrationStatus, e);
            // success will still be false
        }
        if (!success) {
            // could be false from exception or expectation that old values are still there
            log.warn("Failed to update table migration status in DynamoDB to {}.", newTableMigrationStatus);
            return false;
        } else {
            log.info("Updated table migration status in the leader lock item to {}.", newTableMigrationStatus);
            return true;
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

    /**
     * Creates the coordinator table (config.tableName()) if it does not exist; called when not using lease table
     * @throws DependencyException
     */
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
        return Collections.singletonMap(partitionKeyName, DynamoUtils.createAttributeValue(key));
    }

    private CoordinatorState fromDynamoRecord(final Map<String, AttributeValue> dynamoRecord) {
        final HashMap<String, AttributeValue> attributes = new HashMap<>(dynamoRecord);
        final String keyValue = DynamoUtils.safeGetString(attributes.remove(partitionKeyName));

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

        // add partition key (different depending on which table we're writing to)
        result.putAll(getCoordinatorStateKey(state.getKey()));

        // use different serializer depending on which subclass of CoordinatorState this is
        if (state instanceof MigrationState) {
            result.putAll(((MigrationState) state).serialize());
        }
        if (state instanceof StreamInfo) {
            result.putAll(((StreamInfo) state).serialize());
        }

        // add all other attributes besides entityType and partition key to the map and return it
        if (!CollectionUtils.isNullOrEmpty(state.getAttributes())) {
            result.putAll(state.getAttributes());
        }
        return result;
    }

    private Map<String, ExpectedAttributeValue> getDynamoNonExistentExpectation() {
        final Map<String, ExpectedAttributeValue> result = new HashMap<>();

        final ExpectedAttributeValue expectedAV =
                ExpectedAttributeValue.builder().exists(false).build();
        result.put(partitionKeyName, expectedAV);

        return result;
    }

    private Map<String, ExpectedAttributeValue> getDynamoExistentExpectation(final String keyValue) {
        final Map<String, ExpectedAttributeValue> result = new HashMap<>();

        final ExpectedAttributeValue expectedAV = ExpectedAttributeValue.builder()
                .value(AttributeValue.fromS(keyValue))
                .build();
        result.put(partitionKeyName, expectedAV);

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
