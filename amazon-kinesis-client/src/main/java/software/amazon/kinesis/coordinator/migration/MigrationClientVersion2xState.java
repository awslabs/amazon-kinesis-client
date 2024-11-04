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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import lombok.NonNull;
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
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X;
import static software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl.FAULT_METRIC;
import static software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl.METRICS_OPERATION;

/**
 * State for CLIENT_VERSION_2X. In this state, the only allowed valid transition is
 * the roll-forward scenario which can only be performed using the KCL Migration tool.
 * So when the state machine enters this state, a monitor is started to detect the
 * roll-forward scenario.
 */
@KinesisClientInternalApi
@RequiredArgsConstructor
@Slf4j
@ThreadSafe
public class MigrationClientVersion2xState implements MigrationClientVersionState {
    private final MigrationStateMachine stateMachine;
    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ScheduledExecutorService stateMachineThreadPool;
    private final DynamicMigrationComponentsInitializer initializer;
    private final Random random;

    private ClientVersionChangeMonitor rollForwardMonitor;
    private boolean entered = false;
    private boolean left = false;

    @Override
    public ClientVersion clientVersion() {
        return CLIENT_VERSION_2X;
    }

    @Override
    public synchronized void enter(final ClientVersion fromClientVersion) {
        if (!entered) {
            log.info("Entering {} from {}", this, fromClientVersion);
            initializer.initializeClientVersionFor2x(fromClientVersion);

            log.info("Starting roll-forward monitor");
            rollForwardMonitor = new ClientVersionChangeMonitor(
                    initializer.metricsFactory(),
                    coordinatorStateDAO,
                    stateMachineThreadPool,
                    this::onClientVersionChange,
                    clientVersion(),
                    random);
            rollForwardMonitor.startMonitor();
            entered = true;
        } else {
            log.info("Not entering {}", left ? "already exited state" : "already entered state");
        }
    }

    @Override
    public synchronized void leave() {
        if (entered && !left) {
            log.info("Leaving {}", this);
            cancelRollForwardMonitor();
            left = false;
        } else {
            log.info("Cannot leave {}", entered ? "already exited state" : "because state is not active");
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Callback handler to handle client version changes in MigrationState in DDB.
     * @param newState  current MigrationState read from DDB where client version is not CLIENT_VERSION_2X
     * @throws InvalidStateException    during transition to the next state based on the new ClientVersion
     *                                  or if the new state in DDB is unexpected.
     */
    private synchronized void onClientVersionChange(@NonNull final MigrationState newState)
            throws InvalidStateException, DependencyException {
        if (!entered || left) {
            log.warn("Received client version change notification on inactive state {}", this);
            return;
        }
        final MetricsScope scope =
                MetricsUtil.createMetricsWithOperation(initializer.metricsFactory(), METRICS_OPERATION);
        try {
            if (newState.getClientVersion() == CLIENT_VERSION_UPGRADE_FROM_2X) {
                log.info(
                        "A roll-forward has been initiated for the application. Transition to {}",
                        CLIENT_VERSION_UPGRADE_FROM_2X);
                // If this succeeds, the monitor will cancel itself.
                stateMachine.transitionTo(CLIENT_VERSION_UPGRADE_FROM_2X, newState);
            } else {
                // This should not happen, so throw an exception that allows the monitor to continue monitoring
                // changes, this allows KCL to operate in the current state and keep monitoring until a valid
                // state transition is possible.
                // However, there could be a split brain here, new workers will use DDB value as source of truth,
                // so we could also write back CLIENT_VERSION_2X to DDB to ensure all workers have consistent
                // behavior.
                // Ideally we don't expect modifications to DDB table out of the KCL migration tool scope,
                // so keeping it simple and not writing back to DDB, the error log below would help capture
                // any strange behavior if this happens.
                log.error(
                        "Migration state has invalid client version {}. Transition from {} is not supported",
                        newState,
                        CLIENT_VERSION_2X);
                throw new InvalidStateException(String.format("Unexpected new state %s", newState));
            }
        } catch (final InvalidStateException | DependencyException e) {
            scope.addData(FAULT_METRIC, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            throw e;
        } finally {
            MetricsUtil.endScope(scope);
        }
    }

    private void cancelRollForwardMonitor() {
        if (rollForwardMonitor != null) {
            final ClientVersionChangeMonitor localRollForwardMonitor = rollForwardMonitor;
            CompletableFuture.supplyAsync(() -> {
                log.info("Cancelling roll-forward monitor");
                localRollForwardMonitor.cancel();
                return null;
            });
            rollForwardMonitor = null;
        }
    }
}
