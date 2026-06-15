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
package software.amazon.kinesis.coordinator.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.delegate.LeaseTableCoordinatorStateDAODelegate;
import software.amazon.kinesis.coordinator.delegate.LegacyTableCoordinatorStateDAODelegate;
import software.amazon.kinesis.leader.LeaderLock;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static software.amazon.kinesis.worker.metricstats.WorkerMetricStats.Features.SINGLE_TABLE_MIGRATION;

/**
 * Drives the table migration state machine that consolidates CoordinatorState and WorkerMetricStats
 * from separate DDB tables into the Lease table.
 *
 * <p>This state machine always uses the legacy coordinator DAO for reads and writes of the
 * TableMigrationState. The only exception is the final PENDING → COMPLETE transition where
 * the migration state is written to both the legacy table and the lease table as part of
 * copying all entries.</p>
 *
 * For the logic on how the TableMigrationStatus affects how CoordinatorState and WorkerMetricStats
 * are read or written, refer to the respective DAO java doc.
 *
 * <h2>State transitions:</h2>
 * <pre>
 * (no state in DDB + no legacy table)
 *      |
 *      +---> COMPLETE   [2-to-3 migration short-circuit, no state written]
 *
 * (no state in DDB + legacy table exists + config=true)
 *      |
 *      +---> InvalidStateException  [Phase 2 cannot start without Phase 1]
 *
 * (no state in DDB + legacy table exists + config=false)
 *      |
 *      +---> (local INIT, no DDB write yet)
 *               |
 *               | leader: writes INIT to DDB when min support code is met (sets steadySinceEpoch)
 *               | leader: after bake time with min support code still met transition to DEPLOYED
 *               v
 *            DEPLOYED   [Phase 1 complete, safe steady state]
 *               |
 *               | leader (config=true): legacy worker metrics empty
 *               | leader: starts async move of coordinator state entries to lease table
 *               | leader: after async move completes + legacy table verified empty
 *               | leader: writes PENDING to DDB (starts bake timer)
 *               | leader: after bake time with min support code still met transition to PENDING
 *               v
 *            PENDING ----------------------------+
 *               |                                |
 *               | leader: legacy still empty     | leader: legacy non-empty
 *               | + bake time elapsed            |   (Phase 2 rollback detected)
 *               v                                v
 *            COMPLETE                         DEPLOYED
 * </pre>
 *
 * <h2>DEPLOYED → PENDING transition:</h2>
 * <p>When all workers are emitting metrics only to the lease table (DDB TableMigrationStatus=DEPLOYED), the leader:</p>
 * <ol>
 *   <li>Asynchronously moves all coordinator state entries (except the lock and migration state)
 *       from the legacy table to the lease table using transactional batches with leader fencing.</li>
 *   <li>After the async move completes, verifies the legacy table is empty (except lock and
 *       migration state entries).</li>
 *   <li>Writes PENDING to the legacy DDB table, starting the bake timer.</li>
 * </ol>
 *
 * <h2>PENDING → COMPLETE transition:</h2>
 * <p>When the bake time elapses in PENDING state (validating that reads from the lease table
 * work correctly), the leader:</p>
 * <ol>
 *   <li>Creates the TableMigrationState with COMPLETE status conditionally in both
 *       the legacy table and the lease table.</li>
 *   <li>Updates the {@link TableMigrationStatusProvider} to COMPLETE.</li>
 *   <li>Throws {@link InvalidStateException} to signal the caller to release the current
 *       leader lock (held on legacy table) so the next cycle acquires the lock from
 *       the lease table.</li>
 * </ol>
 *
 * <h2>Worker behavior (config-driven):</h2>
 * <p>Workers determine their effective local status from DDB state + config. Only COMPLETE from
 * DDB changes behavior unconditionally. All other states are overridden by config:</p>
 * Refer to <@code determineAndPersistStatus()> javadoc for more details.
 *
 * <h2>Rollback handling:</h2>
 * <h3>Phase 2 to Phase 1 (PENDING to DEPLOYED):</h3>
 * <p>Rolled-back workers (config=false) resume writing to legacy table. The leader in PENDING detects
 * non-empty legacy metrics and writes DEPLOYED back to DDB. From DEPLOYED, the leader re-evaluates
 * once all workers converge before re-entering PENDING.</p>
 *
 * <h3>Phase 1 to 3.4 (no guaranteed reset):</h3>
 * <p>The 3.4 worker has no table migration code. On rollback, DDB state remains as-is. On
 * rollforward, Phase 1 code evaluates from whatever state was left. Customers should not initiate
 * a second deployment until the first has fully succeeded across all workers.</p>
 *
 * ThreadSafety - This class methods are called by primarily called from
 * LeaderDecider from Scheduler thread and from LAM thread via the LAMDataManager.
 */
@Slf4j
@KinesisClientInternalApi
@ThreadSafe
public class TableMigrationStateMachineImpl implements TableMigrationStateMachine {

    private final TableMigrationStatusProvider statusProvider;
    private final CoordinatorStateDAO coordinatorStateDAO;
    private final LegacyTableCoordinatorStateDAODelegate legacyDao;
    private final LeaseTableCoordinatorStateDAODelegate leaseTableDao;
    /** WorkerId to perform Leader Fencing based on Leader lock owner */
    private final String workerId;

    private final boolean migrateAllEntitiesToLeaseTable;
    /** Override map of configured bake time for migration status transitions */
    private final Map<TableMigrationStatus, Long> bakeTimeOverrides;

    /**
     * Tracks an in-progress async move operation. If non-null and complete,
     * the next handlePendingState call will finalize the transition.
     * Access is guarded by {@code synchronized}.
     */
    private CompletableFuture<Boolean> pendingMoveFuture;

    /**
     * Access is guarded by {@code synchronized}.
     */
    private boolean initialized = false;

    /**
     * Latest migration summary pushed by the LAMDataProvider's consumer callback.
     * Updated on each LAM run (leader only). Used by {@code handleLeaderLockResult}
     * to evaluate whether all workers have migrated their metrics to the lease table.
     * Access is guarded by {@code synchronized}.
     */
    private TableMigrationSummary latestMigrationSummary;

    public TableMigrationStateMachineImpl(
            final TableMigrationStatusProvider statusProvider,
            final CoordinatorStateDAO coordinatorStateDAO,
            final String workerId,
            final CoordinatorConfig coordinatorConfig) {
        this.statusProvider = statusProvider;
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.legacyDao = coordinatorStateDAO.getLegacyTableDaoDelegate();
        this.leaseTableDao = coordinatorStateDAO.getLeaseTableDaoDelegate();
        this.workerId = workerId;
        this.migrateAllEntitiesToLeaseTable = coordinatorConfig.migrateAllEntitiesToLeaseTable();
        this.bakeTimeOverrides = buildBakeTimeOverrides(coordinatorConfig);
    }

    private static Map<TableMigrationStatus, Long> buildBakeTimeOverrides(final CoordinatorConfig config) {
        final Map<TableMigrationStatus, Long> overrides = new HashMap<>();
        overrides.put(
                TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE,
                config.effectiveTableMigrationCompleteBakeTimeSeconds());
        return overrides;
    }

    // ==================== Lifecycle ====================

    /**
     * Initialize TableMigrationStateMachine, there can be dependency exception
     * when updating TableMigrationState in DDB or InvalidStateException when
     * 2 phase deployment is not followed. In both case retry will be attempted
     * from Scheduler.initialize and finally fail the application from starting
     * after max retries.
     */
    @Override
    public synchronized void initialize() throws DependencyException, InvalidStateException {
        if (initialized) {
            return;
        }

        log.info(
                "Initializing TableMigrationStateMachine (migrateAllEntitiesToLeaseTable={})",
                migrateAllEntitiesToLeaseTable);

        // 1. Let delegates initialize based on table existence so reads work before we know
        // the table migration status.
        coordinatorStateDAO.initializeDelegates();

        // 2. Read current state from legacy DDB table.
        final TableMigrationState stateFromDDB = readStateFromLegacy();

        // 3. Determine current status and perform any worker-driven transition + write.
        final TableMigrationStatus effectiveStatus = determineStatusForInitialization(stateFromDDB);

        // 4. Publish status so routing and downstream components work.
        statusProvider.initialize(
                effectiveStatus != TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, effectiveStatus);

        // 5. Initialize and enable reads and writes in the DAO (status is no longer UNKNOWN).
        coordinatorStateDAO.initialize();

        initialized = true;
        log.info("TableMigrationStateMachine initialized: {}", effectiveStatus);
    }

    @Override
    public synchronized void handleLeaderLockResult(final boolean isLeader)
            throws DependencyException, InvalidStateException {
        if (!initialized) {
            throw new IllegalStateException("TableMigrationStateMachine not initialized");
        }
        if (statusProvider.getTableMigrationStatus() == TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE) {
            return;
        }

        // Refresh from legacy DDB and re-apply config override to determine effective local status.
        // This ensures we detect external state advances (e.g., another leader wrote DEPLOYED)
        // and apply the config override consistently (e.g., DDB=PENDING + config=false → DEPLOYED).
        // If we cannot read from DDB we dont know if we have the correct lock so we propagate
        // DependencyException which will return isLeader = false.
        final TableMigrationState stateFromDDB = readStateFromLegacy();
        if (stateFromDDB != null) {
            final TableMigrationStatus statusFromDDB = stateFromDDB.getTableMigrationStatus();
            if (statusFromDDB == TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE) {
                statusProvider.updateTableMigrationStatus(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
                // Migration completed by another leader. Throw to release current lock so the
                // next cycle acquires lock from lease table.
                throw new InvalidStateException(
                        "Table migration completed by another leader. Release lock to acquire from lease table on next call.");
            }
            // Okay to ignore exceptions here because we will only remain in INIT state and
            // dont need to force the leader to give up the lock because all worker will
            // run into it
            final TableMigrationStatus effectiveStatus = applyConfigOverrideSafe(statusFromDDB);
            if (effectiveStatus != statusProvider.getTableMigrationStatus()) {
                log.info(
                        "Refreshed local status: {} -> {} (DDB={})",
                        statusProvider.getTableMigrationStatus(),
                        effectiveStatus,
                        statusFromDDB);
                statusProvider.updateTableMigrationStatus(effectiveStatus);
                // lock is still valid since lock changes only on transition to COMPLETED
            }
        }

        if (!isLeader) {
            // If we lost leadership while an async move is in progress, cancel it.
            if (pendingMoveFuture != null && !pendingMoveFuture.isDone()) {
                log.info("Lost leadership, cancelling in-progress async move.");
                pendingMoveFuture.cancel(true);
                pendingMoveFuture = null;
            }
            return;
        }

        // Leader uses local effective status to drive state transitions.
        // Each handler reads the DDB state (passed in) to perform conditional writes.
        switch (statusProvider.getTableMigrationStatus()) {
            case TABLE_MIGRATION_STATUS_INIT:
                handleInitState(stateFromDDB);
                break;
            case TABLE_MIGRATION_STATUS_DEPLOYED:
                handleDeployedState(stateFromDDB);
                break;
            case TABLE_MIGRATION_STATUS_PENDING:
                handlePendingState(stateFromDDB);
                break;
            default:
                break;
        }
    }

    /**
     * Apply config override without throwing. If the combination is invalid (e.g., INIT + config=true),
     * return the DDB value as-is since this is a periodic refresh — the InvalidStateException was
     * already thrown during initialization.
     */
    private TableMigrationStatus applyConfigOverrideSafe(final TableMigrationStatus statusFromDDB) {
        try {
            return applyConfigOverride(statusFromDDB);
        } catch (final InvalidStateException e) {
            log.warn(
                    "Invalid state during refresh (DDB={}, config={}), using DDB value as-is",
                    statusFromDDB,
                    migrateAllEntitiesToLeaseTable);
            return statusFromDDB;
        }
    }

    // ==================== Initialization ====================

    /**
     * Determine the effective local status from DDB state and config
     *
     * <p>Logic:</p>
     * <ul>
     *   <li>If DDB has COMPLETE → return COMPLETE (terminal, config irrelevant).</li>
     *   <li>If no state in DDB and no legacy table → return COMPLETE to DDB (2→3 short-circuit).</li>
     *   <li>If no state in DDB and legacy table exists → use config to determine local status.</li>
     *   <li>If DDB has a non-COMPLETE state → apply config override per the table below.</li>
     * </ul>
     *
     * <p>Config-based override table (workers override DDB value based on their config):</p>
     * <pre>
     * DDB state  | config (migrateAll.   | effective local status
     *            | entitiesToLeaseTable) |
     * -----------+-----------------------+------------------------
     * none       | false                 | INIT
     * INIT       | false                 | INIT
     * none       | true                  | InvalidStateException
     * INIT       | true                  | InvalidStateException
     * DEPLOYED   | false                 | DEPLOYED
     * PENDING    | false                 | DEPLOYED
     * DEPLOYED   | true                  | PENDING
     * PENDING    | true                  | PENDING
     * COMPLETE   | any                   | COMPLETE
     * </pre>
     */
    private TableMigrationStatus determineStatusForInitialization(final TableMigrationState stateFromDDB)
            throws DependencyException, InvalidStateException {

        // Case 1: No state in DDB.
        if (stateFromDDB == null) {
            if (!legacyDao.isEnabled()) {
                // No legacy table — 2→3 migration short-circuit
                log.info("No state in DDB, no legacy table -> writing COMPLETE (2->3 migration)");
                // Dont persist table migration status, we could be in the process of 2->3 migration
                // and we cannot write to lease table yet, checking if legacy table exists is cheap
                // so continue to just use COMPLETE status in memory and all workers will have the
                // same ando nce we toggle to v3 functionality, the workers will start using lease
                // table for all entity types
                return TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE;
            }
            // Legacy table exists — config determines local status (leader will persist later).
            if (migrateAllEntitiesToLeaseTable) {
                throw new InvalidStateException("Cannot deploy Phase 2 (migrateAllEntitiesToLeaseTable=true) "
                        + "without deploying Phase 1 first. No table migration state exists in DDB, "
                        + "indicating Phase 1 was never deployed.");
            }
            log.info("No state in DDB, legacy exists, config=false -> local INIT");
            return TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT;
        }

        // Case 2: DDB has a state — apply config override.
        final TableMigrationStatus statusFromDDB = stateFromDDB.getTableMigrationStatus();

        if (statusFromDDB == TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE) {
            return TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE;
        }

        // Apply config-based override.
        final TableMigrationStatus effectiveStatus = applyConfigOverride(statusFromDDB);
        log.info(
                "DDB state={}, config migrateAll={}, effective local status={}",
                statusFromDDB,
                migrateAllEntitiesToLeaseTable,
                effectiveStatus);
        return effectiveStatus;
    }

    /**
     * Apply config-based override to a DDB status.
     * - INIT + config=true → InvalidStateException (Phase 2 cannot start before Phase 1 completes)
     * - DEPLOYED + config=true → PENDING (Phase 2 worker sees DEPLOYED, uses PENDING locally)
     * - PENDING + config=false → DEPLOYED (Phase 1 worker sees PENDING from rollback, stays DEPLOYED)
     * - All others: use DDB value as-is.
     */
    private TableMigrationStatus applyConfigOverride(final TableMigrationStatus statusFromDDB)
            throws InvalidStateException {
        if (statusFromDDB == TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT && migrateAllEntitiesToLeaseTable) {
            throw new InvalidStateException("Cannot deploy Phase 2 (migrateAllEntitiesToLeaseTable=true) "
                    + "while Phase 1 is still in INIT. Phase 1 must reach DEPLOYED first.");
        }
        if (statusFromDDB == TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED && migrateAllEntitiesToLeaseTable) {
            return TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING;
        }
        if (statusFromDDB == TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING && !migrateAllEntitiesToLeaseTable) {
            return TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED;
        }
        return statusFromDDB;
    }

    // ==================== Leader State Handlers ====================

    /**
     * Handle INIT state as leader (local status = INIT, config = false).
     *
     * <p>The leader monitors for min support code across all workers. When met, it writes INIT
     * to DDB (modifiedTimestamp serves as bake start). After bake time elapses, advances to DEPLOYED.</p>
     *
     * <p>If min support code is NOT met but DDB already has INIT (e.g., a Phase 1 rollback to 3.4
     * happened and then rolled forward — min support code regressed), the leader deletes the state
     * as best effort so the bake restarts fresh on next convergence. Note: during actual rollback
     * the 3.4 worker has no code to delete state, so this cleanup only happens on rollforward
     * if the leader rolls forward before rest of the worker, thats why is best effort and not
     * guaranteed.</p>
     *
     * Called from synchronized context.
     */
    private void handleInitState(final TableMigrationState stateFromDDB) {
        if (!isMinSupportCodeMet()) {
            if (stateFromDDB != null) {
                // Min support code regressed (rollback scenario). Delete state so bake restarts.
                // Best-effort: during actual rollback, 3.4 workers can't do this.
                log.info("INIT: min support code NOT met but DDB has INIT — deleting state (best effort).");
                deleteStateSafe(stateFromDDB);
            }
            log.debug("INIT: waiting for min support code across all workers.");
            return;
        }

        // Min support code met.
        if (stateFromDDB == null) {
            // First time min support code is met — write INIT to legacy DDB.
            // The modifiedTimestamp on this write serves as the bake start time.
            log.info("INIT: min support code met, writing INIT to legacy DDB");
            // If we encounter error while writing to DDB, no need to fail isLeader call
            // we can retry writing next time.
            writeStatusToLegacySafe(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, null);
            return;
        }

        // State exists — use modifiedTimestamp (millis) converted to seconds as bake start.
        final long steadySinceSeconds = TimeUnit.MILLISECONDS.toSeconds(stateFromDDB.getModifiedTimestamp());
        final long nowSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        final long elapsedSeconds = nowSeconds - steadySinceSeconds;
        final long bakeTimeSeconds =
                TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED.getBakeTimeSeconds(bakeTimeOverrides);

        if (elapsedSeconds >= bakeTimeSeconds) {
            log.info("INIT -> DEPLOYED: bake time elapsed ({}s >= {}s)", elapsedSeconds, bakeTimeSeconds);
            if (writeStatusToLegacySafe(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, stateFromDDB)) {
                // only update status if we are able to successfully write to DDB.
                statusProvider.updateTableMigrationStatus(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);
            }
        } else {
            log.debug("INIT: baking ({}s / {}s)", elapsedSeconds, bakeTimeSeconds);
        }
    }

    /**
     * Handle DEPLOYED state as leader (local status = DEPLOYED, config = false).
     *
     * <p>Local DEPLOYED means config=false (Phase 1 worker). This is the steady state for Phase 1.
     * The leader does nothing except ensure DDB state is consistent:</p>
     * <ul>
     *   <li>If DDB state is PENDING (left over from a previous Phase 2 attempt that was rolled back),
     *       write DEPLOYED back to DDB to reset it. Because transition to PENDING only happens
     *       when all workers had written worker metric stats to lease table atleast once, the fact
     *       that this worker has a false config indicate a rollback to phase1</li>
     *   <li>Otherwise: no-op, waiting for Phase 2 deployment.</li>
     * </ul>
     */
    private void handleDeployedState(final TableMigrationState stateFromDDB) {
        if (stateFromDDB != null
                && stateFromDDB.getTableMigrationStatus() == TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING) {
            // DDB has PENDING but we're locally DEPLOYED (config=false → Phase 1 rollback).
            // Write DEPLOYED back to legacy DDB.
            log.info("DEPLOYED: DDB has PENDING but config=false, writing DEPLOYED back to legacy DDB.");
            writeStatusToLegacySafe(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, stateFromDDB);
        } else {
            log.debug("DEPLOYED: Phase 1 leader, steady state. Waiting for Phase 2 deployment.");
        }
    }

    /**
     * Handle PENDING state as leader (local status = PENDING, config = true).
     *
     * <p>Local PENDING means config=true (Phase 2 worker). The DDB state could be DEPLOYED or PENDING.
     * The leader drives the DDB state forward through the DEPLOYED→PENDING→COMPLETE progression:</p>
     * <ul>
     *   <li>If DDB = DEPLOYED: check if legacy worker metrics are empty. If yes, start the async
     *       move of coordinator state entries to the lease table. Once the async move completes
     *       successfully, write PENDING to DDB (transitions DDB from DEPLOYED→PENDING, starting the
     *       bake timer). This ensures all data is in the lease table before the bake period begins.</li>
     *   <li>If DDB = PENDING: check if legacy worker metrics are still empty.
     *       If NOT empty (rollback detected): write DEPLOYED back to DDB (but don't change local
     *       status — we're still Phase 2 locally, we just need DDB to reflect that not all workers
     *       have converged yet).
     *       If empty: check bake time. After bake time elapses, finalize transition to COMPLETE.
     *       The bake period validates that reading from the lease table works correctly before
     *       marking the migration complete. bcause all coordinator state entries should be
     *       moved by then.</li>
     * </ul>
     *
     * @throws InvalidStateException when the PENDING → COMPLETE transition succeeds, signaling
     *         the caller to release the current lock so the correct lock can be acquired
     */
    private void handlePendingState(final TableMigrationState stateFromDDB) throws InvalidStateException {
        if (stateFromDDB == null) {
            // No DDB state — should not happen normally (requires DDB=DEPLOYED or PENDING to be locally
            // in PENDING). This can only occur if state was manually deleted. Create DEPLOYED state so
            // the state machine re-evaluates from the correct starting point.
            log.warn("PENDING: no state in DDB (manually deleted?). Creating DEPLOYED state to unblock.");
            writeStatusToLegacySafe(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, null);
            return;
        }

        final TableMigrationStatus statusFromDDB = stateFromDDB.getTableMigrationStatus();

        if (statusFromDDB == TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED) {
            // DDB is DEPLOYED, advance to PENDING if LegacyWorkerMetrics table is empty (all workers are writing
            // to lease table) and moving coordinator State to lease table has finished.
            if (!isLegacyWorkerMetricsEmpty()) {
                // Legacy metrics is non-empty — if an async move is in progress, cancel it (rollback detected).
                if (pendingMoveFuture != null && !pendingMoveFuture.isDone()) {
                    log.info("PENDING: DDB=DEPLOYED, legacy non-empty — cancelling in-progress async move.");
                    pendingMoveFuture.cancel(true);
                }
                pendingMoveFuture = null;
                log.debug("PENDING: DDB=DEPLOYED, waiting for all workers to emit metrics to lease table.");
                return;
            }

            // All workers are emiting to lease table only — start or check async move.
            if (pendingMoveFuture != null) {
                if (!pendingMoveFuture.isDone()) {
                    log.debug("PENDING: DDB=DEPLOYED, async move still in progress, waiting...");
                    return;
                }
                // Move finished — check result
                try {
                    final boolean moveSuccess = pendingMoveFuture.join();
                    if (moveSuccess) {
                        // Verify the legacy table is actually empty (except lock + migration status)
                        // before transitioning to PENDING. This guards against partial moves or
                        // entries written concurrently during the async move.
                        if (!isLegacyCoordinatorStateEmpty()) {
                            log.warn("PENDING: async move reported success but legacy table still has entries. "
                                    + "Will retry next cycle.");
                            pendingMoveFuture = null;
                            return;
                        }
                        log.info("PENDING: async move succeeded and legacy table verified empty, "
                                + "writing PENDING to legacy DDB to start bake.");
                        if (writeStatusToLegacySafe(
                                TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING, stateFromDDB)) {
                            // if we cannot update DDB we can reevaluate the status of the future
                            // in the next handleLeaderLockResult call
                            pendingMoveFuture = null;
                        }
                    } else {
                        log.warn("PENDING: async move reported failure, will retry next cycle.");
                        pendingMoveFuture = null;
                    }
                } catch (final Exception e) {
                    log.warn("PENDING: async move threw exception, will retry next cycle.", e);
                    pendingMoveFuture = null;
                }
            } else {
                // No in-flight move — start the async move.
                log.info("PENDING: DDB=DEPLOYED, legacy empty — starting async move of entries to lease table.");
                startAsyncMove();
            }
            return;
        }

        if (statusFromDDB == TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING) {
            if (!isLegacyWorkerMetricsEmpty() || !isLegacyCoordinatorStateEmpty()) {
                // Some worker resumed writing to legacy — rollback detected.
                // This means we also may have new coordinator state entries in legacy table which requires
                // to be moved to lease table.
                // Write DEPLOYED back to legacy DDB. Don't change local status (still PENDING/Phase 2).
                log.info("PENDING: DDB=PENDING but legacy non-empty — writing DEPLOYED to legacy DDB (rollback).");
                writeStatusToLegacySafe(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, stateFromDDB);
                // Cancel any in-progress move since we're rolling back.
                if (pendingMoveFuture != null && !pendingMoveFuture.isDone()) {
                    pendingMoveFuture.cancel(true);
                }
                pendingMoveFuture = null;
                return;
            }

            // Legacy is empty — check bake time. The bake period validates that reading from
            // the lease table (where data was already moved) works correctly before marking complete.
            final long steadySinceSeconds = TimeUnit.MILLISECONDS.toSeconds(stateFromDDB.getModifiedTimestamp());
            final long nowSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            final long elapsedSeconds = nowSeconds - steadySinceSeconds;
            final long bakeTimeSeconds =
                    TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE.getBakeTimeSeconds(bakeTimeOverrides);

            if (elapsedSeconds >= bakeTimeSeconds) {
                log.info(
                        "PENDING -> COMPLETE: bake time elapsed ({}s >= {}s), finalizing transition.",
                        elapsedSeconds,
                        bakeTimeSeconds);
                finalizeMigrationComplete(stateFromDDB);
                // finalizeMigrationComplete throws InvalidStateException — we won't reach here
            } else {
                log.debug(
                        "PENDING: baking ({}s / {}s), validating lease table reads work correctly.",
                        elapsedSeconds,
                        bakeTimeSeconds);
            }
        }
    }

    // ==================== Async Copy Logic ====================

    /**
     * Starts asynchronous move of all coordinator state entries (except the lock and
     * TableMigrationState) from the legacy table to the lease table using the common ForkJoinPool.
     * Each invocation re-fetches entries from legacy table, so retries after failure will pick up
     * the current state of the legacy table.
     */
    private void startAsyncMove() {
        pendingMoveFuture = CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return moveCoordinatorStateEntries();
                    } catch (final Exception e) {
                        log.error("Async move of coordinator state entries failed", e);
                        return false;
                    }
                },
                ForkJoinPool.commonPool());
    }

    /**
     * Maximum number of entries per transactional batch. Each entry produces 2 TransactWriteItems
     * (one Put to lease table + one Delete from legacy table), so the effective DDB transaction
     * size is BATCH_SIZE * 2. DDB allows up to 100 items per transaction, so 25 entries = 50 items.
     */
    private static final int TRANSACTION_BATCH_SIZE = 25;

    /**
     * Moves all coordinator state entries from the legacy table to the lease table in
     * transactional batches. Each batch atomically writes entries to the lease table
     * (conditional on not already existing) and deletes them from the legacy table.
     * deletes are safe because currently none of the coordinator state is updated.
     * if they can be updated we would need to conditional delete if the state in
     * legacy did not change.
     *
     * <p>Entries excluded from the move:</p>
     * <ul>
     *   <li>Lock entries (LEADER_HASH_KEY, DEPLOYING_LEADER_HASH_KEY) — remain in legacy</li>
     *   <li>TableMigrationState entry — written separately with COMPLETE status</li>
     * </ul>
     *
     * <p>TODO: Transaction failure due to a conditional check failure should not
     * happen because we are doing a transaction move and coordinator state
     * entries are unique and will only be present in one table and never in both
     * but if this happens we have to handle it and check if its the same copy
     * and unblock migration to continue, if the entry is different possibly
     * use the last modified time to pick the latest .</p>
     *
     * This is not a critical section and does not access any variables that
     * are used in other critical section.
     * So its safe to run without synchronization.
     *
     * @return true if all entries were moved successfully, false otherwise
     */
    private boolean moveCoordinatorStateEntries()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final List<CoordinatorState> legacyEntries = legacyDao.listCoordinatorState();
        final List<CoordinatorState> entriesToMove = new ArrayList<>();

        for (final CoordinatorState entry : legacyEntries) {
            // Skip lock entries — they are not migrated
            if (LeaderLock.LEADER_HASH_KEY.equals(entry.getKey())
                    || CoordinatorState.DEPLOYING_LEADER_HASH_KEY.equals(entry.getKey())) {
                log.debug("Skipping lock entry: {}", entry.getKey());
                continue;
            }
            // Skip the migration state itself — it will be written separately with COMPLETE status
            if (TableMigrationState.TABLE_MIGRATION_HASH_KEY.equals(entry.getKey())) {
                log.debug("Skipping migration state entry — will be written with COMPLETE status");
                continue;
            }
            entriesToMove.add(entry);
        }

        log.info(
                "Moving {} coordinator state entries from legacy to lease table in batches of {}",
                entriesToMove.size(),
                TRANSACTION_BATCH_SIZE);

        // Process in batches, checking for interruption between each batch so that
        // cancel(true) from the main thread (e.g., on rollback or leadership loss) stops
        // the move promptly rather than continuing to process remaining batches.
        for (int i = 0; i < entriesToMove.size(); i += TRANSACTION_BATCH_SIZE) {
            if (Thread.currentThread().isInterrupted()) {
                log.info(
                        "Async move interrupted after processing {} of {} entries, aborting.", i, entriesToMove.size());
                return false;
            }
            final int end = Math.min(i + TRANSACTION_BATCH_SIZE, entriesToMove.size());
            final List<CoordinatorState> batch = entriesToMove.subList(i, end);
            moveBatch(batch);
        }

        log.info("Successfully moved all {} coordinator state entries to lease table", entriesToMove.size());
        return true;
    }

    /**
     * Moves a single batch of entries transactionally. The transaction includes:
     * - A ConditionCheck on the lock entry to verify this worker still holds leadership
     * - A conditional Put per entry into the lease table (must not already exist)
     * - A Delete per entry from the legacy table
     *
     * <p>If the transaction fails (leadership lost or conditional put fails), the entire
     * async move operation fails and will be retried on the next leader cycle, which
     * re-fetches fresh entries from legacy.</p>
     *
     * @throws DependencyException if the transaction fails for any reason
     */
    private void moveBatch(final List<CoordinatorState> batch)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        // +1 for the lock ownership condition check
        final List<TransactWriteItem> transactItems = new ArrayList<>(batch.size() * 2 + 1);

        // Leader fencing: assert this worker still owns the lock.
        // If another worker acquired the lock, the entire transaction is rejected atomically.
        transactItems.add(createLockOwnerConditionCheck());

        for (final CoordinatorState entry : batch) {
            // Put to lease table (conditional: item must not already exist)
            transactItems.add(leaseTableDao.createTransactPut(entry));
            // Delete from legacy table
            transactItems.add(legacyDao.createTransactDelete(entry.getKey()));
        }

        coordinatorStateDAO.executeTransactWrite(transactItems);
        log.info("Transactionally moved batch of {} entries (with lock fencing)", batch.size());
    }

    /**
     * Creates a DDB ConditionCheck TransactWriteItem that asserts the leader lock entry
     * in the legacy table is still owned by this worker. The DDB lock client stores the
     * lock owner in the "ownerName" attribute.
     *
     * <p>Including this in each transactional batch ensures that if leadership was lost
     * (another worker took the lock), the entire transaction fails atomically — preventing
     * partial moves by a stale leader.</p>
     *
     * TODO: Handle DEPLOYING_LEADER_HASH_KEY coordinator state.
     */
    private TransactWriteItem createLockOwnerConditionCheck() {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put(legacyDao.getPartitionKeyAttributeName(), AttributeValue.fromS(LeaderLock.LEADER_HASH_KEY));

        final Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(":owner", AttributeValue.fromS(workerId));

        final ConditionCheck check = ConditionCheck.builder()
                .tableName(legacyDao.getTableName())
                .key(key)
                .conditionExpression("ownerName = :owner")
                .expressionAttributeValues(exprValues)
                .build();
        return TransactWriteItem.builder().conditionCheck(check).build();
    }

    /**
     * Finalizes the PENDING → COMPLETE transition after async copy is done.
     *
     * <p>This method:</p>
     * <ol>
     *   <li>Creates the TableMigrationState with COMPLETE status in the lease table
     *       and Updates the existing state in the legacy table to COMPLETE
     *       (conditional on the previous status still being PENDING) transactionally.</li>
     *   <li>Updates the {@link TableMigrationStatusProvider} to COMPLETE.</li>
     *   <li>Throws {@link InvalidStateException} to release the lock.</li>
     * </ol>
     *
     * @param existingState the current PENDING state from the legacy table
     * @throws InvalidStateException always thrown on success to signal lock release
     */
    private void finalizeMigrationComplete(final TableMigrationState existingState) throws InvalidStateException {
        // Build COMPLETE state for both tables
        final TableMigrationState leaseTableState = existingState.copy();
        leaseTableState.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, workerId);

        final TableMigrationState updatedLegacy =
                existingState.update(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, workerId);

        // Atomically write COMPLETE to both the lease table (conditional create) and legacy table
        // (conditional on current status being PENDING). This ensures both are set together.
        final List<TransactWriteItem> transactItems = new ArrayList<>(3);

        // Leader fencing: assert we still own the lock
        transactItems.add(createLockOwnerConditionCheck());

        // Create COMPLETE in lease table (must not already exist)
        transactItems.add(leaseTableDao.createTransactPut(leaseTableState));

        // Overwrite legacy entry with COMPLETE (conditional on existing status = PENDING).
        // Uses Put with condition expression: the item must exist AND its status must be PENDING.
        final Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(
                ":expectedStatus", AttributeValue.fromS(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING.name()));
        final Put legacyPut = Put.builder()
                .tableName(legacyDao.getTableName())
                .item(legacyDao.toTransactRecord(updatedLegacy))
                .conditionExpression(TableMigrationState.TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME + " = :expectedStatus")
                .expressionAttributeValues(exprValues)
                .build();
        transactItems.add(TransactWriteItem.builder().put(legacyPut).build());

        try {
            coordinatorStateDAO.executeTransactWrite(transactItems);
            log.info("Transactionally wrote COMPLETE to both lease table and legacy table.");
        } catch (final DependencyException e) {
            // Transaction failed — either leadership lost or conditional check failed
            log.warn("Failed to write COMPLETE transactionally ({}). Will retry next cycle.", e.getMessage());
            return;
        }

        // Update status provider
        // Note: entries were already deleted from legacy during the transactional move.
        // The legacy table now only contains the lock entry and the TableMigrationState (COMPLETE).
        statusProvider.updateTableMigrationStatus(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        log.info(
                "Table migration COMPLETE. Legacy table cleaned. Release legacy lock for lease table lock acquisition.");

        // Throw InvalidStateException to signal the caller to release the current lock.
        // The next isLeader check will use the COMPLETE status to acquire lock from lease table.
        throw new InvalidStateException(
                "Table migration completed. Release legacy lock so lease table lock can be acquired.");
    }

    // ==================== Migration Stats Consumer ====================

    /**
     * Consumer method to receive migration summary from the LAMDataProvider.
     * Called on the LAM thread each time data is loaded. Thread-safe via synchronized.
     *
     * <p>This is the method reference passed as {@code Consumer<TableMigrationSummary>} when
     * constructing the {@link software.amazon.kinesis.coordinator.assignment.MigrationAwareLAMDataManager}.</p>
     */
    public synchronized void updateMigrationSummary(final TableMigrationSummary summary) {
        this.latestMigrationSummary = summary;
        log.debug("Updated migration summary: {}", summary);
    }

    // ==================== Condition Checks ====================

    /**
     * Returns true when all workers in the fleet emit the minimum support code in their
     * WorkerMetricStats entries, indicating they have the Phase 1 (3.5) code deployed.
     *
     * <p>Uses the minSupportCode from the latest migration summary (computed by
     * {@link software.amazon.kinesis.coordinator.assignment.MigrationAwareLAMDataManager}).
     * The min support code must be >= SINGLE_TABLE_MIGRATION ordinal for this to return true.</p>
     */
    private synchronized boolean isMinSupportCodeMet() {
        if (latestMigrationSummary == null) {
            log.debug("No migration summary available yet, conservatively returning false for min support code.");
            return false;
        }
        final int minSupport = latestMigrationSummary.getMinSupportCode();
        final int required = SINGLE_TABLE_MIGRATION.ordinal();
        final boolean met = minSupport >= required;
        log.info("isMinSupportCodeMet={} (minSupportCode={}, required={})", met, minSupport, required);
        return met;
    }

    /**
     * Returns true when the legacy coordinator state table contains only the expected
     * entries that are not moved: the leader lock, deploying leader lock, and the
     * TableMigrationState entry. Any other entries indicate the async move did not
     * fully complete or new entries were written concurrently.
     *
     * <p>This is called after the async move reports success to confirm the legacy table
     * is actually clean before transitioning DDB to PENDING.</p>
     */
    private boolean isLegacyCoordinatorStateEmpty() {
        try {
            final List<CoordinatorState> entries = legacyDao.listCoordinatorState();
            for (final CoordinatorState entry : entries) {
                final String key = entry.getKey();
                if (LeaderLock.LEADER_HASH_KEY.equals(key)
                        || CoordinatorState.DEPLOYING_LEADER_HASH_KEY.equals(key)
                        || TableMigrationState.TABLE_MIGRATION_HASH_KEY.equals(key)) {
                    continue;
                }
                // Found an unexpected entry — legacy is not empty.
                log.info("isLegacyCoordinatorStateEmpty: found unexpected entry '{}' in legacy table.", key);
                return false;
            }
            return true;
        } catch (final InvalidStateException | ProvisionedThroughputException | DependencyException e) {
            log.warn("Failed to list coordinator state from legacy table", e);
            return false;
        }
    }

    /**
     * Returns true when all workers have fully migrated their metrics to the lease table,
     * meaning:
     * <ol>
     *   <li>Total workers with leases == total active workers with metrics (all workers are reporting)</li>
     *   <li>Active workers with metrics in legacy table == 0 (no one is writing to legacy anymore)</li>
     * </ol>
     *
     * <p>Uses the latest migration stats pushed by LAMDataProvider to avoid additional DDB calls.
     * Returns false if no stats are available yet (LAM hasn't run), as a conservative default.</p>
     */
    private synchronized boolean isLegacyWorkerMetricsEmpty() {
        if (latestMigrationSummary == null) {
            log.debug("No migration summary available yet, conservatively returning false.");
            return false;
        }
        final int workersWithLeases = latestMigrationSummary.getWorkersWithUnexpiredLeases();
        final boolean allWorkersReporting =
                workersWithLeases == latestMigrationSummary.getTotalActiveWorkersWithMetrics();
        final boolean legacyEmpty = latestMigrationSummary.getActiveWorkersWithMetricsInLegacyTable() == 0;
        final boolean result = allWorkersReporting && legacyEmpty;

        log.info(
                "isLegacyWorkerMetricsEmpty={} (workersWithLeases={}, totalActiveWithMetrics={}, activeInLegacy={})",
                result,
                workersWithLeases,
                latestMigrationSummary.getTotalActiveWorkersWithMetrics(),
                latestMigrationSummary.getActiveWorkersWithMetricsInLegacyTable());
        return result;
    }

    // ==================== DDB Operations ====================

    /**
     * Read state from the legacy table only. The state machine always uses the legacy
     * coordinator DAO for its own state management.
     */
    private TableMigrationState readStateFromLegacy() throws DependencyException {
        try {
            final CoordinatorState state = legacyDao.getCoordinatorState(TableMigrationState.TABLE_MIGRATION_HASH_KEY);
            if (state instanceof TableMigrationState) {
                return (TableMigrationState) state;
            }
            return null;
        } catch (final InvalidStateException | ProvisionedThroughputException e) {
            throw new DependencyException("Failed to read table migration state from legacy table", e);
        }
    }

    /**
     * Delete state from legacy DDB. Best-effort — logs a warning on failure but does not throw.
     * Used during Phase 1 rollback detection to reset the bake timer.
     */
    private void deleteStateSafe(final TableMigrationState state) {
        try {
            legacyDao.deleteCoordinatorState(state.getKey());
            log.info("Deleted table migration state from legacy (best effort reset). Previous state: {}", state);
        } catch (final ProvisionedThroughputException | InvalidStateException | DependencyException e) {
            log.warn(
                    "Failed to delete table migration state from legacy (best effort), will retry next cycle. State: {}",
                    state,
                    e);
        }
    }

    private boolean writeStatusToLegacySafe(
            final TableMigrationStatus status, final TableMigrationState existingState) {
        try {
            writeStatusToLegacy(status, existingState);
            return true;
        } catch (DependencyException e) {
            log.warn("Unable to write table migration status {}, existing state {}", status, existingState, e);
        }
        return false;
    }

    /**
     * Write status to the legacy DDB table.
     * - If existingState is null: creates the entry (conditional on key not existing).
     * - If existingState is non-null: updates conditionally (key exists AND previous status matches).
     */
    private void writeStatusToLegacy(final TableMigrationStatus status, final TableMigrationState existingState)
            throws DependencyException {
        try {
            if (existingState == null) {
                final TableMigrationState newState = new TableMigrationState(workerId);
                newState.update(status, workerId);
                if (legacyDao.createCoordinatorStateIfNotExists(newState)) {
                    log.info("Created table migration state in legacy: {}", status);
                } else {
                    log.info("Table migration state already exists in legacy, another worker created it.");
                }
            } else {
                final TableMigrationState updated = existingState.copy();
                updated.update(status, workerId);
                if (legacyDao.updateCoordinatorStateWithExpectation(
                        updated, existingState.getDynamoTableMigrationStatusExpectation())) {
                    log.info("Updated table migration state in legacy: {}", status);
                } else {
                    log.warn("Conditional write to legacy failed for {}. Another worker advanced state.", status);
                }
            }
        } catch (final InvalidStateException | ProvisionedThroughputException e) {
            throw new DependencyException("Failed to write table migration status " + status + " to legacy", e);
        }
    }
}
