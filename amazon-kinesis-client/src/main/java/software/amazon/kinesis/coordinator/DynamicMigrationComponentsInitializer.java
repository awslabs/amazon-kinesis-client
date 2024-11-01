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
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider.LeaseAssignmentMode;
import software.amazon.kinesis.coordinator.assignment.LeaseAssignmentManager;
import software.amazon.kinesis.coordinator.migration.ClientVersion;
import software.amazon.kinesis.leader.DynamoDBLockBasedLeaderDecider;
import software.amazon.kinesis.leader.MigrationAdaptiveLeaderDecider;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsManager;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsReporter;

import static software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider.LeaseAssignmentMode.DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT;
import static software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider.LeaseAssignmentMode.WORKER_UTILIZATION_AWARE_ASSIGNMENT;
import static software.amazon.kinesis.coordinator.assignment.LeaseAssignmentManager.DEFAULT_NO_OF_SKIP_STAT_FOR_DEAD_WORKER_THRESHOLD;

/**
 * This class is responsible for initializing the KCL components that supports
 * seamless upgrade from v2.x to v3.x.
 * During specific versions, it also dynamically switches the functionality
 * to be either vanilla 3.x or 2.x compatible.
 *
 * It is responsible for creating:
 * 1. LeaderDecider
 * 2. LAM
 * 3. WorkerMetricStatsReporter
 *
 * It manages initializing the following components at initialization time
 * 1. workerMetricsDAO and workerMetricsManager
 * 2. leaderDecider
 * 3. MigrationAdaptiveLeaseAssignmentModeProvider
 *
 * It updates the following components dynamically:
 * 1. starts/stops LAM
 * 2. starts/stops WorkerMetricStatsReporter
 * 3. updates LeaseAssignmentMode to either DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT or WORKER_UTILIZATION_AWARE_ASSIGNMENT
 * 4. creates GSI (deletion is done by KclMigrationTool)
 * 5. creates WorkerMetricStats table (deletion is done by KclMigrationTool)
 * 6. updates LeaderDecider to either DeterministicShuffleShardSyncLeaderDecider or DynamoDBLockBasedLeaderDecider
 */
@Slf4j
@KinesisClientInternalApi
@ThreadSafe
@Accessors(fluent = true)
public final class DynamicMigrationComponentsInitializer {
    private static final long SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS = 60L;

    @Getter
    private final MetricsFactory metricsFactory;

    @Getter
    private final LeaseRefresher leaseRefresher;

    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ScheduledExecutorService workerMetricsThreadPool;

    @Getter
    private final WorkerMetricStatsDAO workerMetricsDAO;

    private final WorkerMetricStatsManager workerMetricsManager;
    private final ScheduledExecutorService lamThreadPool;
    private final BiFunction<ScheduledExecutorService, LeaderDecider, LeaseAssignmentManager> lamCreator;
    private final Supplier<MigrationAdaptiveLeaderDecider> adaptiveLeaderDeciderCreator;
    private final Supplier<DeterministicShuffleShardSyncLeaderDecider> deterministicLeaderDeciderCreator;
    private final Supplier<DynamoDBLockBasedLeaderDecider> ddbLockBasedLeaderDeciderCreator;

    @Getter
    private final String workerIdentifier;

    private final WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig;

    @Getter
    private final long workerMetricsExpirySeconds;

    private final MigrationAdaptiveLeaseAssignmentModeProvider leaseModeChangeConsumer;

    @Getter
    private LeaderDecider leaderDecider;

    private LeaseAssignmentManager leaseAssignmentManager;
    private ScheduledFuture<?> workerMetricsReporterFuture;
    private LeaseAssignmentMode currentAssignmentMode;
    private boolean dualMode;
    private boolean initialized;

    @Builder(access = AccessLevel.PACKAGE)
    DynamicMigrationComponentsInitializer(
            final MetricsFactory metricsFactory,
            final LeaseRefresher leaseRefresher,
            final CoordinatorStateDAO coordinatorStateDAO,
            final ScheduledExecutorService workerMetricsThreadPool,
            final WorkerMetricStatsDAO workerMetricsDAO,
            final WorkerMetricStatsManager workerMetricsManager,
            final ScheduledExecutorService lamThreadPool,
            final BiFunction<ScheduledExecutorService, LeaderDecider, LeaseAssignmentManager> lamCreator,
            final Supplier<MigrationAdaptiveLeaderDecider> adaptiveLeaderDeciderCreator,
            final Supplier<DeterministicShuffleShardSyncLeaderDecider> deterministicLeaderDeciderCreator,
            final Supplier<DynamoDBLockBasedLeaderDecider> ddbLockBasedLeaderDeciderCreator,
            final String workerIdentifier,
            final WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig,
            final MigrationAdaptiveLeaseAssignmentModeProvider leaseAssignmentModeProvider) {
        this.metricsFactory = metricsFactory;
        this.leaseRefresher = leaseRefresher;
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.workerIdentifier = workerIdentifier;
        this.workerUtilizationAwareAssignmentConfig = workerUtilizationAwareAssignmentConfig;
        this.workerMetricsExpirySeconds = Duration.ofMillis(DEFAULT_NO_OF_SKIP_STAT_FOR_DEAD_WORKER_THRESHOLD
                        * workerUtilizationAwareAssignmentConfig.workerMetricsReporterFreqInMillis())
                .getSeconds();
        this.workerMetricsManager = workerMetricsManager;
        this.workerMetricsDAO = workerMetricsDAO;
        this.workerMetricsThreadPool = workerMetricsThreadPool;
        this.lamThreadPool = lamThreadPool;
        this.lamCreator = lamCreator;
        this.adaptiveLeaderDeciderCreator = adaptiveLeaderDeciderCreator;
        this.deterministicLeaderDeciderCreator = deterministicLeaderDeciderCreator;
        this.ddbLockBasedLeaderDeciderCreator = ddbLockBasedLeaderDeciderCreator;
        this.leaseModeChangeConsumer = leaseAssignmentModeProvider;
    }

    public void initialize(final ClientVersion migrationStateMachineStartingClientVersion) throws DependencyException {
        if (initialized) {
            log.info("Already initialized, nothing to do");
            return;
        }

        // always collect metrics so that when we flip to start reporting we will have accurate historical data.
        log.info("Start collection of WorkerMetricStats");
        workerMetricsManager.startManager();
        if (migrationStateMachineStartingClientVersion == ClientVersion.CLIENT_VERSION_3X) {
            initializeComponentsFor3x();
        } else {
            initializeComponentsForMigration(migrationStateMachineStartingClientVersion);
        }
        log.info("Initialized dual mode {} current assignment mode {}", dualMode, currentAssignmentMode);

        log.info("Creating LAM");
        leaseAssignmentManager = lamCreator.apply(lamThreadPool, leaderDecider);
        log.info("Initializing {}", leaseModeChangeConsumer.getClass().getSimpleName());
        leaseModeChangeConsumer.initialize(dualMode, currentAssignmentMode);
        initialized = true;
    }

    private void initializeComponentsFor3x() {
        log.info("Initializing for 3x functionality");
        dualMode = false;
        currentAssignmentMode = WORKER_UTILIZATION_AWARE_ASSIGNMENT;
        log.info("Initializing dualMode {} assignmentMode {}", dualMode, currentAssignmentMode);
        leaderDecider = ddbLockBasedLeaderDeciderCreator.get();
        log.info("Initializing {}", leaderDecider.getClass().getSimpleName());
        leaderDecider.initialize();
    }

    private void initializeComponentsForMigration(final ClientVersion migrationStateMachineStartingClientVersion) {
        log.info("Initializing for migration to 3x");
        dualMode = true;
        final LeaderDecider initialLeaderDecider;
        if (migrationStateMachineStartingClientVersion == ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK) {
            currentAssignmentMode = WORKER_UTILIZATION_AWARE_ASSIGNMENT;
            initialLeaderDecider = ddbLockBasedLeaderDeciderCreator.get();
        } else {
            currentAssignmentMode = DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT;
            initialLeaderDecider = deterministicLeaderDeciderCreator.get();
        }
        log.info("Initializing dualMode {} assignmentMode {}", dualMode, currentAssignmentMode);

        final MigrationAdaptiveLeaderDecider adaptiveLeaderDecider = adaptiveLeaderDeciderCreator.get();
        log.info(
                "Initializing MigrationAdaptiveLeaderDecider with {}",
                initialLeaderDecider.getClass().getSimpleName());
        adaptiveLeaderDecider.updateLeaderDecider(initialLeaderDecider);
        this.leaderDecider = adaptiveLeaderDecider;
    }

    void shutdown() {
        log.info("Shutting down components");
        if (initialized) {
            log.info("Stopping LAM, LeaderDecider, workerMetrics reporting and collection");
            leaseAssignmentManager.stop();
            // leader decider is shut down later when scheduler is doing a final shutdown
            // since scheduler still accesses the leader decider while shutting down
            stopWorkerMetricsReporter();
            workerMetricsManager.stopManager();
        }

        // lam does not manage lifecycle of its threadpool to easily stop/start dynamically.
        // once migration code is obsolete (i.e. all 3x functionality is the baseline and no
        // migration is needed), it can be moved inside lam
        log.info("Shutting down lamThreadPool and workerMetrics reporter thread pool");
        lamThreadPool.shutdown();
        workerMetricsThreadPool.shutdown();
        try {
            if (!lamThreadPool.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                lamThreadPool.shutdownNow();
            }
        } catch (final InterruptedException e) {
            log.warn("Interrupted while waiting for shutdown of LeaseAssignmentManager ThreadPool", e);
            lamThreadPool.shutdownNow();
        }

        try {
            if (!workerMetricsThreadPool.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                workerMetricsThreadPool.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for shutdown of WorkerMetricStatsManager ThreadPool", e);
            workerMetricsThreadPool.shutdownNow();
        }
    }

    private void startWorkerMetricsReporting() throws DependencyException {
        if (workerMetricsReporterFuture != null) {
            log.info("Worker metrics reporting is already running...");
            return;
        }
        log.info("Initializing WorkerMetricStats");
        this.workerMetricsDAO.initialize();
        log.info("Starting worker metrics reporter");
        // Start with a delay for workerStatsManager to capture some values and start reporting.
        workerMetricsReporterFuture = workerMetricsThreadPool.scheduleAtFixedRate(
                new WorkerMetricStatsReporter(metricsFactory, workerIdentifier, workerMetricsManager, workerMetricsDAO),
                workerUtilizationAwareAssignmentConfig.inMemoryWorkerMetricsCaptureFrequencyMillis() * 2L,
                workerUtilizationAwareAssignmentConfig.workerMetricsReporterFreqInMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void stopWorkerMetricsReporter() {
        log.info("Stopping worker metrics reporter");
        if (workerMetricsReporterFuture != null) {
            workerMetricsReporterFuture.cancel(false);
            workerMetricsReporterFuture = null;
        }
    }

    /**
     * Create LeaseOwnerToLeaseKey GSI for the lease table
     * @param blockingWait  whether to wait for the GSI creation or not, if false, the gsi creation will be initiated
     *                      but this call will not block for its creation
     * @throws DependencyException  If DDB fails unexpectedly when creating the GSI
     */
    private void createGsi(final boolean blockingWait) throws DependencyException {
        log.info("Creating Lease table GSI if it does not exist");
        // KCLv3.0 always starts with GSI available
        leaseRefresher.createLeaseOwnerToLeaseKeyIndexIfNotExists();

        if (blockingWait) {
            log.info("Waiting for Lease table GSI creation");
            final long secondsBetweenPolls = 10L;
            final long timeoutSeconds = 600L;
            final boolean isIndexActive =
                    leaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(secondsBetweenPolls, timeoutSeconds);

            if (!isIndexActive) {
                throw new DependencyException(
                        new IllegalStateException("Creating LeaseOwnerToLeaseKeyIndex on Lease table timed out"));
            }
        }
    }

    /**
     * Initialize KCL with components and configuration to support upgrade from 2x. This can happen
     * at KCL Worker startup when MigrationStateMachine starts in ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X.
     * Or Dynamically during roll-forward from ClientVersion.CLIENT_VERSION_2X.
     */
    public synchronized void initializeClientVersionForUpgradeFrom2x(final ClientVersion fromClientVersion)
            throws DependencyException {
        log.info("Initializing KCL components for upgrade from 2x from {}", fromClientVersion);

        createGsi(false);
        startWorkerMetricsReporting();
        // LAM is not started until the dynamic flip to 3xWithRollback
    }

    /**
     * Initialize KCL with components and configuration to run vanilla 3x functionality. This can happen
     * at KCL Worker startup when MigrationStateMachine starts in ClientVersion.CLIENT_VERSION_3X, or dynamically
     * during a new deployment when existing worker are in ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK
     */
    public synchronized void initializeClientVersionFor3x(final ClientVersion fromClientVersion)
            throws DependencyException {
        log.info("Initializing KCL components for 3x from {}", fromClientVersion);

        log.info("Initializing LeaseAssignmentManager, DDB-lock-based leader decider, WorkerMetricStats manager"
                + " and creating the Lease table GSI if it does not exist");
        if (fromClientVersion == ClientVersion.CLIENT_VERSION_INIT) {
            // gsi may already exist and be active for migrated application.
            createGsi(true);
            startWorkerMetricsReporting();
            log.info("Starting LAM");
            leaseAssignmentManager.start();
        }
        // nothing to do when transitioning from CLIENT_VERSION_3X_WITH_ROLLBACK.
    }

    /**
     * Initialize KCL with components and configuration to run 2x compatible functionality
     * while allowing roll-forward. This can happen at KCL Worker startup when MigrationStateMachine
     * starts in ClientVersion.CLIENT_VERSION_2X (after a rollback)
     * Or Dynamically during rollback from CLIENT_VERSION_UPGRADE_FROM_2X or CLIENT_VERSION_3X_WITH_ROLLBACK.
     */
    public synchronized void initializeClientVersionFor2x(final ClientVersion fromClientVersion) {
        log.info("Initializing KCL components for rollback to 2x from {}", fromClientVersion);

        if (fromClientVersion != ClientVersion.CLIENT_VERSION_INIT) {
            // dynamic rollback
            stopWorkerMetricsReporter();
            // Migration Tool will delete the lease table LeaseOwner GSI
            // and WorkerMetricStats table
        }

        if (fromClientVersion == ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK) {
            // we are rolling back after flip
            currentAssignmentMode = DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT;
            notifyLeaseAssignmentModeChange();
            log.info("Stopping LAM");
            leaseAssignmentManager.stop();
            final LeaderDecider leaderDecider = deterministicLeaderDeciderCreator.get();
            if (this.leaderDecider instanceof MigrationAdaptiveLeaderDecider) {
                log.info(
                        "Updating LeaderDecider to {}", leaderDecider.getClass().getSimpleName());
                ((MigrationAdaptiveLeaderDecider) this.leaderDecider).updateLeaderDecider(leaderDecider);
            } else {
                throw new IllegalStateException(String.format("Unexpected leader decider %s", this.leaderDecider));
            }
        }
    }

    /**
     * Initialize KCL with components and configuration to run vanilla 3x functionality
     * while allowing roll-back to 2x functionality. This can happen at KCL Worker startup
     * when MigrationStateMachine starts in ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK (after the flip)
     * Or Dynamically during flip from CLIENT_VERSION_UPGRADE_FROM_2X.
     */
    public synchronized void initializeClientVersionFor3xWithRollback(final ClientVersion fromClientVersion)
            throws DependencyException {
        log.info("Initializing KCL components for 3x with rollback from {}", fromClientVersion);

        if (fromClientVersion == ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X) {
            // dynamic flip
            currentAssignmentMode = WORKER_UTILIZATION_AWARE_ASSIGNMENT;
            notifyLeaseAssignmentModeChange();
            final LeaderDecider leaderDecider = ddbLockBasedLeaderDeciderCreator.get();
            log.info("Updating LeaderDecider to {}", leaderDecider.getClass().getSimpleName());
            ((MigrationAdaptiveLeaderDecider) this.leaderDecider).updateLeaderDecider(leaderDecider);
        } else {
            startWorkerMetricsReporting();
        }

        log.info("Starting LAM");
        leaseAssignmentManager.start();
    }

    /**
     * Synchronously invoke the consumer to change the lease assignment mode.
     */
    private void notifyLeaseAssignmentModeChange() {
        if (dualMode) {
            log.info("Notifying {} of {}", leaseModeChangeConsumer, currentAssignmentMode);
            if (Objects.nonNull(leaseModeChangeConsumer)) {
                try {
                    leaseModeChangeConsumer.updateLeaseAssignmentMode(currentAssignmentMode);
                } catch (final Exception e) {
                    log.warn("LeaseAssignmentMode change consumer threw exception", e);
                }
            }
        } else {
            throw new IllegalStateException("Unexpected assignment mode change");
        }
    }
}
