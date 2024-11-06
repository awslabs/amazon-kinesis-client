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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.DynamicMigrationComponentsInitializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_2X;
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK;
import static software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl.FAULT_METRIC;
import static software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl.METRICS_OPERATION;

/**
 * State for CLIENT_VERSION_UPGRADE_FROM_2X. When state machine enters this state,
 * KCL is initialized to operate in dual mode for Lease assignment and Leader decider algorithms
 * which initially start in 2.x compatible mode and when all the KCL workers are 3.x compliant,
 * it dynamically switches to the 3.x algorithms. It also monitors for rollback
 * initiated from customer via the KCL migration tool and instantly switches back to the 2.x
 * complaint algorithms.
 * The allowed state transitions are to CLIENT_VERSION_3X_WITH_ROLLBACK when KCL workers are
 * 3.x complaint, and to CLIENT_VERSION_2X when customer has initiated a rollback.
 * Only the leader KCL worker performs migration ready monitor and notifies all workers (including
 * itself) via a MigrationState update. When all worker's monitor notice the MigrationState change
 * (including itself), it will transition to CLIENT_VERSION_3X_WITH_ROLLBACK.
 */
@KinesisClientInternalApi
@RequiredArgsConstructor
@Slf4j
@ThreadSafe
public class MigrationClientVersionUpgradeFrom2xState implements MigrationClientVersionState {
    private final MigrationStateMachine stateMachine;
    private final Callable<Long> timeProvider;
    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ScheduledExecutorService stateMachineThreadPool;
    private final DynamicMigrationComponentsInitializer initializer;
    private final Random random;
    private final MigrationState currentMigrationState;
    private final long flipTo3XStabilizerTimeInSeconds;

    private MigrationReadyMonitor migrationMonitor;
    private ClientVersionChangeMonitor clientVersionChangeMonitor;
    private boolean entered = false;
    private boolean left = false;

    @Override
    public ClientVersion clientVersion() {
        return ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X;
    }

    @Override
    public synchronized void enter(final ClientVersion fromClientVersion) throws DependencyException {
        if (!entered) {
            log.info("Entering state {} from {}", this, fromClientVersion);
            initializer.initializeClientVersionForUpgradeFrom2x(fromClientVersion);

            log.info("Starting migration ready monitor to monitor 3.x compliance of the KCL workers");
            migrationMonitor = new MigrationReadyMonitor(
                    initializer.metricsFactory(),
                    timeProvider,
                    initializer.leaderDecider(),
                    initializer.workerIdentifier(),
                    initializer.workerMetricsDAO(),
                    initializer.workerMetricsExpirySeconds(),
                    initializer.leaseRefresher(),
                    stateMachineThreadPool,
                    this::onMigrationReady,
                    flipTo3XStabilizerTimeInSeconds);
            migrationMonitor.startMonitor();

            log.info("Starting monitor for rollback and flip to 3.x");
            clientVersionChangeMonitor = new ClientVersionChangeMonitor(
                    initializer.metricsFactory(),
                    coordinatorStateDAO,
                    stateMachineThreadPool,
                    this::onClientVersionChange,
                    clientVersion(),
                    random);
            clientVersionChangeMonitor.startMonitor();
            entered = true;
        } else {
            log.info("Not entering {}", left ? "already exited state" : "already entered state");
        }
    }

    @Override
    public synchronized void leave() {
        if (entered && !left) {
            log.info("Leaving {}", this);
            cancelMigrationReadyMonitor();
            cancelClientChangeVersionMonitor();
            entered = false;
        } else {
            log.info("Cannot leave {}", entered ? "already exited state" : "because state is not active");
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private synchronized void onMigrationReady() {
        // this is invoked on the leader worker only
        if (!entered || left || migrationMonitor == null) {
            log.info("Ignoring migration ready monitor, state already transitioned");
            return;
        }
        // update dynamo with the state to toggle to 3.x
        // and let the clientVersionChange kick in to do state transition
        // this way both leader and non-leader worker all transition when
        // it discovers the update from ddb.
        if (updateDynamoStateForTransition()) {
            // successfully toggled the state, now we can cancel the monitor
            cancelMigrationReadyMonitor();
        }
        // else - either migration ready monitor will retry or
        // client Version change callback will initiate the next state transition.
    }

    private void cancelMigrationReadyMonitor() {
        if (migrationMonitor != null) {
            final MigrationReadyMonitor localMigrationMonitor = migrationMonitor;
            CompletableFuture.supplyAsync(() -> {
                log.info("Cancelling migration ready monitor");
                localMigrationMonitor.cancel();
                return null;
            });
            migrationMonitor = null;
        }
    }

    private void cancelClientChangeVersionMonitor() {
        if (clientVersionChangeMonitor != null) {
            final ClientVersionChangeMonitor localClientVersionChangeMonitor = clientVersionChangeMonitor;
            CompletableFuture.supplyAsync(() -> {
                log.info("Cancelling client change version monitor");
                localClientVersionChangeMonitor.cancel();
                return null;
            });
            clientVersionChangeMonitor = null;
        }
    }

    /**
     * Callback handler to handle client version changes in MigrationState in DDB.
     * @param newState  current MigrationState read from DDB where client version is not CLIENT_VERSION_UPGRADE_FROM_2X
     * @throws InvalidStateException    during transition to the next state based on the new ClientVersion
     *                                  or if the new state in DDB is unexpected.
     */
    private synchronized void onClientVersionChange(final MigrationState newState)
            throws InvalidStateException, DependencyException {
        if (!entered || left) {
            log.warn("Received client version change notification on inactive state {}", this);
            return;
        }
        final MetricsScope scope =
                MetricsUtil.createMetricsWithOperation(initializer.metricsFactory(), METRICS_OPERATION);
        try {
            switch (newState.getClientVersion()) {
                case CLIENT_VERSION_2X:
                    log.info("A rollback has been initiated for the application. Transition to {}", CLIENT_VERSION_2X);
                    // cancel monitor asynchronously
                    cancelMigrationReadyMonitor();
                    stateMachine.transitionTo(CLIENT_VERSION_2X, newState);
                    break;
                case CLIENT_VERSION_3X_WITH_ROLLBACK:
                    log.info("KCL workers are v3.x compliant, transition to {}", CLIENT_VERSION_3X_WITH_ROLLBACK);
                    cancelMigrationReadyMonitor();
                    stateMachine.transitionTo(CLIENT_VERSION_3X_WITH_ROLLBACK, newState);
                    break;
                default:
                    // This should not happen, so throw an exception that allows the monitor to continue monitoring
                    // changes, this allows KCL to operate in the current state and keep monitoring until a valid
                    // state transition is possible.
                    // However, there could be a split brain here, new workers will use DDB value as source of truth,
                    // so we could also write back CLIENT_VERSION_UPGRADE_FROM_2X to DDB to ensure all workers have
                    // consistent behavior.
                    // Ideally we don't expect modifications to DDB table out of the KCL migration tool scope,
                    // so keeping it simple and not writing back to DDB, the error log below would help capture
                    // any strange behavior if this happens.
                    log.error("Migration state has invalid client version {}", newState);
                    throw new InvalidStateException(String.format("Unexpected new state %s", newState));
            }
        } catch (final DependencyException | InvalidStateException e) {
            scope.addData(FAULT_METRIC, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            throw e;
        } finally {
            MetricsUtil.endScope(scope);
        }
    }

    private boolean updateDynamoStateForTransition() {
        final MetricsScope scope =
                MetricsUtil.createMetricsWithOperation(initializer.metricsFactory(), METRICS_OPERATION);
        try {
            final MigrationState newMigrationState = currentMigrationState
                    .copy()
                    .update(CLIENT_VERSION_3X_WITH_ROLLBACK, initializer.workerIdentifier());
            log.info("Updating Migration State in DDB with {} prev state {}", newMigrationState, currentMigrationState);
            return coordinatorStateDAO.updateCoordinatorStateWithExpectation(
                    newMigrationState, currentMigrationState.getDynamoClientVersionExpectation());
        } catch (final Exception e) {
            log.warn(
                    "Exception occurred when toggling to {}, upgradeReadyMonitor will retry the update"
                            + " if upgrade condition is still true",
                    CLIENT_VERSION_3X_WITH_ROLLBACK,
                    e);
            scope.addData(FAULT_METRIC, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            return false;
        } finally {
            MetricsUtil.endScope(scope);
        }
    }
}
