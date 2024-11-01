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
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_3X;
import static software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl.FAULT_METRIC;
import static software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl.METRICS_OPERATION;

/**
 * State for CLIENT_VERSION_3X_WITH_ROLLBACK which enables KCL to run its 3.x compliant algorithms
 * during the upgrade process after all KCL workers in the fleet are 3.x complaint. Since this
 * is an instant switch from CLIENT_VERSION_UPGRADE_FROM_2X, it also supports rollback if customers
 * see regression to allow for instant rollbacks as well. This would be achieved by customers
 * running a KCL migration tool to update MigrationState in DDB. So this state monitors for
 * rollback triggers and performs state transitions accordingly.
 */
@Slf4j
@KinesisClientInternalApi
@RequiredArgsConstructor
@ThreadSafe
public class MigrationClientVersion3xWithRollbackState implements MigrationClientVersionState {

    private final MigrationStateMachine stateMachine;
    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ScheduledExecutorService stateMachineThreadPool;
    private final DynamicMigrationComponentsInitializer initializer;
    private final Random random;

    private ClientVersionChangeMonitor rollbackMonitor;
    private boolean entered;
    private boolean left;

    @Override
    public ClientVersion clientVersion() {
        return ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK;
    }

    @Override
    public synchronized void enter(final ClientVersion fromClientVersion) throws DependencyException {
        if (!entered) {
            log.info("Entering {} from {}", this, fromClientVersion);
            initializer.initializeClientVersionFor3xWithRollback(fromClientVersion);
            // we need to run the rollback monitor
            log.info("Starting rollback monitor");
            rollbackMonitor = new ClientVersionChangeMonitor(
                    initializer.metricsFactory(),
                    coordinatorStateDAO,
                    stateMachineThreadPool,
                    this::onClientVersionChange,
                    clientVersion(),
                    random);
            rollbackMonitor.startMonitor();
            entered = true;
        } else {
            log.info("Not entering {}", left ? "already exited state" : "already entered state");
        }
    }

    @Override
    public void leave() {
        if (entered && !left) {
            log.info("Leaving {}", this);
            cancelRollbackMonitor();
            entered = false;
            left = true;
        } else {
            log.info("Cannot leave {}", entered ? "already exited state" : "because state is not active");
        }
    }

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
                    stateMachine.transitionTo(ClientVersion.CLIENT_VERSION_2X, newState);
                    break;
                case CLIENT_VERSION_3X:
                    log.info("Customer has switched to 3.x after successful upgrade, state machine will move to a"
                            + "terminal state and stop monitoring. Rollbacks will no longer be supported anymore");
                    stateMachine.transitionTo(CLIENT_VERSION_3X, newState);
                    // This worker will still be running the migrationAdaptive components in 3.x mode which will
                    // no longer dynamically switch back to 2.x mode, however to directly run 3.x component without
                    // adaption to migration (i.e. move to CLIENT_VERSION_3X state), it requires this worker to go
                    // through the current deployment which initiated the switch to 3.x mode.
                    break;
                default:
                    // This should not happen, so throw an exception that allows the monitor to continue monitoring
                    // changes, this allows KCL to operate in the current state and keep monitoring until a valid
                    // state transition is possible.
                    // However, there could be a split brain here, new workers will use DDB value as source of truth,
                    // so we could also write back CLIENT_VERSION_3X_WITH_ROLLBACK to DDB to ensure all workers have
                    // consistent behavior.
                    // Ideally we don't expect modifications to DDB table out of the KCL migration tool scope,
                    // so keeping it simple and not writing back to DDB, the error log below would help capture
                    // any strange behavior if this happens.
                    log.error("Migration state has invalid client version {}", newState);
                    throw new InvalidStateException(String.format("Unexpected new state %s", newState));
            }
        } catch (final InvalidStateException | DependencyException e) {
            scope.addData(FAULT_METRIC, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            throw e;
        } finally {
            MetricsUtil.endScope(scope);
        }
    }

    private void cancelRollbackMonitor() {
        if (rollbackMonitor != null) {
            final ClientVersionChangeMonitor localRollbackMonitor = rollbackMonitor;
            CompletableFuture.supplyAsync(() -> {
                log.info("Cancelling rollback monitor");
                localRollbackMonitor.cancel();
                return null;
            });
            rollbackMonitor = null;
        }
    }
}
