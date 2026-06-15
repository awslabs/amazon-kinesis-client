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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.ThreadSafe;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.coordinator.delegate.CoordinatorStateDAODelegate;
import software.amazon.kinesis.coordinator.delegate.LeaseTableCoordinatorStateDAODelegate;
import software.amazon.kinesis.coordinator.delegate.LegacyTableCoordinatorStateDAODelegate;
import software.amazon.kinesis.coordinator.migration.TableMigrationStateMachine;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * Data Access Object that routes {@link CoordinatorState} operations to the appropriate
 * DDB table based on the current {@link TableMigrationStatus}.
 *
 * <h2>Read Logic</h2>
 * <ul>
 *   <li>If status != COMPLETE: read from legacy table first; if found, return it. Otherwise read from lease table.
 *       The legacy delegate handles the case where the table doesn't exist (returns null/empty).</li>
 *   <li>If status == COMPLETE: read only from lease table.</li>
 * </ul>
 *
 * <h2>Write Logic</h2>
 * <ul>
 *   <li>cache table migration startup upon initialize(). </li>
 *   <li>If cachedStatus == COMPLETE or PENDING: write to lease table only.</li>
 *   <li>If cachedStatus == INIT or DEPLOYED: write to legacy table only.</li>
 * </ul>
 *
 * The difference between read and write are:
 * 1. Reads are merged from both tables while writes go only to one table. This is
 *    because when status is INIT/DEPLOYED, the application could be on a rollback
 *    + rollforward path, so even if application is currently writing to
 *    Legacy table, in the past workers may have written to Lease table. So its safer
 *    to read both until the final transition to COMPLETED.
 * 2. Reads dynamically flip on completed state, but wokers flip to Lease table
 *    when it reboots. This is to ensure the worker writes move as the deployment
 *    progresses in the cluster fleet.
 *
 * <h2>Special Cases</h2>
 * <ul>
 *   <li>The table migration state machine directly uses the {@link #leaseTableDaoDelegate} field
 *       for its reads/writes and sets the {@link TableMigrationStatusProvider} before calling initialize().</li>
 *   <li>All operations require {@link #initialize()} to have been called first.</li>
 * </ul>
 *
 * <h2>Lock Client</h2>
 * The lock client options builder routes based on status:
 * <ul>
 *   <li>COMPLETE: lease table lock</li>
 *   <li>All others (INIT, DEPLOYED, PENDING, UNKNOWN): legacy table lock</li>
 * </ul>
 * During transition from PENDING to COMPLETE, its possible workers that are caught up to COMPLETE
 * state will grab lock from lease table however those that grab the lock when their local state
 * is still PENDING will grab from the legacy lock. However TableMigrationStateMachine guarantees
 * that it performs a post leadership acquire check where if the local state is different from the
 * DDB state it will force the lock to be relinquished and then return false to is caller on
 * isLeader method. The next isLeader call will use the COMPLETED status to grab the lock correctly.
 * Given that you can never rollback from COMPLETED means that we dont have a rollback scenario
 * to handle correct lock acquision that guarantees only a single leader at a time. Refer to
 * {@link TableMigrationStateMachine.handleLeaderLockResult}
 *
 * <h2>TODO</h2>
 * Update and delete operations currently route based on the table migration status which may not
 * match where the entry was originally read from. If a caller reads from legacy but the status
 * transitions to PENDING/COMPLETE before the update, the update targets the lease table where
 * the entry may not yet exist. Consider tracking read-source per entry and routing to the
 * appropriate delegate based on the source. For now, the routing is best-effort and
 * callers that perform read-then-update must handle conditional check failures gracefully.
 * There is no usecase for an update coordinator state except for the migration state and
 * table migration state. Table Migration state is always updated from state machine using
 * appropriate delegate class directly and never through this DOA.
 * For more details refer to TableMigrationStateMachine.
 * And for migration state the assumption is both state machines are not running concurrently.
 */
@Slf4j
@KinesisClientInternalApi
@ThreadSafe
public class CoordinatorStateDAO {

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final LeaseTableCoordinatorStateDAODelegate leaseTableDaoDelegate;
    private final LegacyTableCoordinatorStateDAODelegate legacyTableDaoDelegate;
    private final TableMigrationStatusProvider tableMigrationStatusProvider;
    private volatile TableMigrationStatus cachedStatus;
    private volatile boolean initialized;

    public LegacyTableCoordinatorStateDAODelegate getLegacyTableDaoDelegate() {
        return legacyTableDaoDelegate;
    }

    public LeaseTableCoordinatorStateDAODelegate getLeaseTableDaoDelegate() {
        return leaseTableDaoDelegate;
    }

    public CoordinatorStateDAO(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            final CoordinatorStateTableConfig coordinatorStateTableConfig,
            final String leaseTableName,
            final TableMigrationStatusProvider tableMigrationStatusProvider) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.leaseTableDaoDelegate = new LeaseTableCoordinatorStateDAODelegate(dynamoDbAsyncClient, leaseTableName);
        this.legacyTableDaoDelegate =
                new LegacyTableCoordinatorStateDAODelegate(dynamoDbAsyncClient, coordinatorStateTableConfig);
        this.tableMigrationStatusProvider = tableMigrationStatusProvider;
        this.initialized = false;
    }

    /**
     * Initialize the delegates for read operations. This must be called early in startup
     * (before the TableMigrationStatusProvider is initialized) so that reads can work
     * during the table migration state machine bootstrap.
     *
     * The legacy delegate checks if the table exists and disables itself if not.
     * This breaks the circular dependency: delegates can serve reads before the
     * TableMigrationStatusProvider knows the final status.
     *
     * @throws DependencyException if unable to determine legacy table existence
     */
    public void initializeDelegates() throws DependencyException {
        legacyTableDaoDelegate.initialize();
        leaseTableDaoDelegate.initialize();
        log.info("CoordinatorStateDAO delegates initialized. Legacy enabled: {}", legacyTableDaoDelegate.isEnabled());
    }

    /**
     * Initialize the DAO for write operations. Must be called after
     * {@link #initializeDelegates()} and after the TableMigrationStatusProvider
     * has moved past UNKNOWN.
     *
     * @throws InvalidStateException if the TableMigrationStatusProvider is still UNKNOWN
     */
    public void initialize() throws InvalidStateException {
        if (initialized) {
            log.info("CoordinatorStateDAO already initialized");
            return;
        }
        cachedStatus = tableMigrationStatusProvider.getTableMigrationStatus();
        if (cachedStatus == TableMigrationStatus.TABLE_MIGRATION_STATUS_UNKNOWN) {
            throw new InvalidStateException(
                    "Cannot initialize CoordinatorStateDAO: TableMigrationStatusProvider is still UNKNOWN");
        }
        initialized = true;
        log.info("CoordinatorStateDAO initialized for writes with cached migration status: {}", cachedStatus);
    }

    // ==================== Read Operations ====================
    // Reads are always allowed after DAO is initialization
    // The legacy delegate handles the disabled case internally (returns null/empty).

    /**
     * Get a single {@link CoordinatorState} by key.
     * Uses the fallback read pattern: if status != COMPLETE, tries legacy first then lease table.
     * If status == COMPLETE, reads only from lease table.
     *
     * @param key the coordinator state key
     * @return the state, or null if not found in any table
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if a required table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public CoordinatorState getCoordinatorState(@NonNull final String key)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ensureInitialized();
        if (isTableMigrationComplete()) {
            return leaseTableDaoDelegate.getCoordinatorState(key);
        }

        // Try legacy first (returns null if disabled), fallback to lease table
        final CoordinatorState legacyResult = legacyTableDaoDelegate.getCoordinatorState(key);
        if (legacyResult != null) {
            return legacyResult;
        }
        return leaseTableDaoDelegate.getCoordinatorState(key);
    }

    /**
     * List all coordinator states.
     * Uses the fallback read pattern with merge: if status != COMPLETE, reads from both tables.
     * Based on the write implementation there should not be any duplicate, if there is, it should
     * be identical and all copies are returned to the caller without deduplication.
     *
     * @return list of all coordinator states
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if a required table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public List<CoordinatorState> listCoordinatorState()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        ensureInitialized();
        if (isTableMigrationComplete()) {
            return leaseTableDaoDelegate.listCoordinatorState();
        }

        // Legacy returns empty list if disabled
        final List<CoordinatorState> result = new ArrayList<>(legacyTableDaoDelegate.listCoordinatorState());
        result.addAll(leaseTableDaoDelegate.listCoordinatorState());

        return result;
    }

    /**
     * List coordinator states by entity type.
     *
     * @param entityType the entity type to filter by
     * @return list of matching coordinator states for the given entityType
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if a required table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public List<CoordinatorState> listCoordinatorStateByEntityType(final EntityType.CoordinatorStateType entityType)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        ensureInitialized();

        if (isTableMigrationComplete()) {
            return leaseTableDaoDelegate.listCoordinatorStateByEntityType(entityType);
        }

        final List<CoordinatorState> result =
                new ArrayList<>(legacyTableDaoDelegate.listCoordinatorStateByEntityType(entityType));
        result.addAll(leaseTableDaoDelegate.listCoordinatorStateByEntityType(entityType));
        return result;
    }

    // ==================== Write Operations ====================
    // Writes require initialization and dont dynamically flip the table target, it only
    // initializes on startup.

    /**
     * Create a coordinator state if it does not already exist.
     *
     * @param state the state to create
     * @return true if created, false if the key already exists
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if not initialized or table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public boolean createCoordinatorStateIfNotExists(@NonNull final CoordinatorState state)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ensureInitialized();
        return getWriteDelegate().createCoordinatorStateIfNotExists(state);
    }

    /**
     * Update a coordinator state with expectations.
     *
     * @param state the state to update
     * @param expectations conditional expectations for the update
     * @return true if updated, false if expectations not met
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if not initialized or table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public boolean updateCoordinatorStateWithExpectation(
            @NonNull final CoordinatorState state, final Map<String, ExpectedAttributeValue> expectations)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ensureInitialized();
        // TODO: Update routing may not match where the entry was read from if status changed.
        // Callers must handle conditional check failures and retry.
        return getWriteDelegate().updateCoordinatorStateWithExpectation(state, expectations);
    }

    /**
     * Delete a coordinator state by key.
     *
     * @param key the key to delete
     * @return true if deleted
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if not initialized or table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public boolean deleteCoordinatorState(@NonNull final String key)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        ensureInitialized();
        // TODO: Delete routing may not match where the entry exists. Same concern as update.
        return getWriteDelegate().deleteCoordinatorState(key);
    }

    // ==================== Lock Client ====================

    /**
     * Get the DDB Lock Client options builder pointing to the appropriate table.
     * Routes based on table migration status:
     * - COMPLETE: lease table lock
     * - All others: legacy table lock
     */
    public AmazonDynamoDBLockClientOptionsBuilder getDDBLockClientOptionsBuilder() {
        if (isTableMigrationComplete()) {
            return leaseTableDaoDelegate.getDDBLockClientOptionsBuilder();
        }
        return legacyTableDaoDelegate.getDDBLockClientOptionsBuilder();
    }

    // ==================== Transactional Operations ====================

    /**
     * Execute a DynamoDB TransactWriteItems request with the given list of {@link TransactWriteItem}s.
     * Used by the table migration state machine to atomically move entries between tables
     * (put into lease table + delete from legacy table in a single transaction).
     *
     * @param transactWriteItems the list of transact write items (max 100 per DDB limit)
     * @throws DependencyException if DDB fails unexpectedly or the transaction is cancelled
     */
    public void executeTransactWrite(final List<TransactWriteItem> transactWriteItems) throws DependencyException {
        final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                .transactItems(transactWriteItems)
                .build();
        try {
            dynamoDbAsyncClient.transactWriteItems(request).get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DependencyException("TransactWriteItems interrupted", e);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof TransactionCanceledException) {
                throw new DependencyException(
                        "TransactWriteItems cancelled: " + ((TransactionCanceledException) cause).cancellationReasons(),
                        cause);
            }
            throw new DependencyException("TransactWriteItems failed", cause);
        }
    }

    // ==================== Private Helpers ====================

    private boolean isTableMigrationComplete() {
        return tableMigrationStatusProvider.getTableMigrationStatus()
                == TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE;
    }

    private CoordinatorStateDAODelegate getWriteDelegate() {
        switch (cachedStatus) {
            case TABLE_MIGRATION_STATUS_COMPLETE:
            case TABLE_MIGRATION_STATUS_PENDING:
                return leaseTableDaoDelegate;
            case TABLE_MIGRATION_STATUS_INIT:
            case TABLE_MIGRATION_STATUS_DEPLOYED:
                return legacyTableDaoDelegate;
            default:
                throw new IllegalStateException("Cannot determine write delegate for cached status: " + cachedStatus);
        }
    }

    private void ensureInitialized() throws InvalidStateException {
        if (!initialized) {
            throw new InvalidStateException("CoordinatorStateDAO is not initialized. Call initialize() first.");
        }
    }
}
