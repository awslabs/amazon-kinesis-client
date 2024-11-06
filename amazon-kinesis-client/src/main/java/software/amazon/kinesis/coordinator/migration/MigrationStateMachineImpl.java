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

import java.util.AbstractMap.SimpleEntry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.DynamicMigrationComponentsInitializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

/**
 * Implementation of {@link MigrationStateMachine}
 */
@KinesisClientInternalApi
@Getter
@Slf4j
@ThreadSafe
public class MigrationStateMachineImpl implements MigrationStateMachine {
    public static final String FAULT_METRIC = "Fault";
    public static final String METRICS_OPERATION = "Migration";

    private static final long THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS = 5L;

    private final MetricsFactory metricsFactory;
    private final Callable<Long> timeProvider;
    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ScheduledExecutorService stateMachineThreadPool;
    private DynamicMigrationComponentsInitializer initializer;
    private final ClientVersionConfig clientVersionConfig;
    private final Random random;
    private final String workerId;
    private final long flipTo3XStabilizerTimeInSeconds;
    private MigrationState startingMigrationState;

    @Getter
    private ClientVersion startingClientVersion;

    private MigrationClientVersionState currentMigrationClientVersionState = new MigrationClientVersionState() {
        @Override
        public ClientVersion clientVersion() {
            return ClientVersion.CLIENT_VERSION_INIT;
        }

        @Override
        public void enter(final ClientVersion fromClientVersion) {
            log.info("Entered {}...", clientVersion());
        }

        @Override
        public void leave() {
            log.info("Left {}...", clientVersion());
        }
    };
    private boolean terminated = false;

    public MigrationStateMachineImpl(
            final MetricsFactory metricsFactory,
            final Callable<Long> timeProvider,
            final CoordinatorStateDAO coordinatorStateDAO,
            final ScheduledExecutorService stateMachineThreadPool,
            final ClientVersionConfig clientVersionConfig,
            final Random random,
            final DynamicMigrationComponentsInitializer initializer,
            final String workerId,
            final long flipTo3XStabilizerTimeInSeconds) {
        this.metricsFactory = metricsFactory;
        this.timeProvider = timeProvider;
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.stateMachineThreadPool = stateMachineThreadPool;
        this.clientVersionConfig = clientVersionConfig;
        this.random = random;
        this.initializer = initializer;
        this.workerId = workerId;
        this.flipTo3XStabilizerTimeInSeconds = flipTo3XStabilizerTimeInSeconds;
    }

    @Override
    public void initialize() throws DependencyException {
        if (startingClientVersion == null) {
            log.info("Initializing MigrationStateMachine");
            coordinatorStateDAO.initialize();
            final MigrationClientVersionStateInitializer startingStateInitializer =
                    new MigrationClientVersionStateInitializer(
                            timeProvider, coordinatorStateDAO, clientVersionConfig, random, workerId);
            final SimpleEntry<ClientVersion, MigrationState> dataForInitialization =
                    startingStateInitializer.getInitialState();
            initializer.initialize(dataForInitialization.getKey());
            transitionTo(dataForInitialization.getKey(), dataForInitialization.getValue());
            startingClientVersion = dataForInitialization.getKey();
            startingMigrationState = dataForInitialization.getValue();
            log.info("MigrationStateMachine initial clientVersion {}", startingClientVersion);
        } else {
            log.info("MigrationStateMachine already initialized with clientVersion {}", startingClientVersion);
        }
    }

    @Override
    public void shutdown() {
        terminate();
        if (!stateMachineThreadPool.isShutdown()) {
            stateMachineThreadPool.shutdown();
            try {
                if (stateMachineThreadPool.awaitTermination(THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.info(
                            "StateMachineThreadPool did not shutdown within {} seconds, forcefully shutting down",
                            THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS);
                    stateMachineThreadPool.shutdownNow();
                }
            } catch (final InterruptedException e) {
                log.info("Interrupted when shutting down StateMachineThreadPool, forcefully shutting down");
                stateMachineThreadPool.shutdownNow();
            }
        }
        log.info("Shutdown successfully");
    }

    @Override
    public synchronized void terminate() {
        if (!terminated && currentMigrationClientVersionState != null) {
            log.info("State machine is about to terminate");
            currentMigrationClientVersionState.leave();
            currentMigrationClientVersionState = null;
            log.info("State machine reached a terminal state.");
            terminated = true;
        }
    }

    @Override
    public synchronized void transitionTo(final ClientVersion nextClientVersion, final MigrationState migrationState)
            throws DependencyException {
        if (terminated) {
            throw new IllegalStateException(String.format(
                    "Cannot transition to %s after state machine is terminated, %s",
                    nextClientVersion.name(), migrationState));
        }

        final MigrationClientVersionState nextMigrationClientVersionState =
                createMigrationClientVersionState(nextClientVersion, migrationState);
        log.info(
                "Attempting to transition from {} to {}",
                currentMigrationClientVersionState.clientVersion(),
                nextClientVersion);
        currentMigrationClientVersionState.leave();

        enter(nextMigrationClientVersionState);
    }

    /**
     * Enter with retry. When entering the state machine for the first time, the caller has retry so exceptions
     * will be re-thrown. Once the state machine has initialized all transitions will be an indefinite retry.
     * It is possible the DDB state has changed by the time enter succeeds but that will occur as a new
     * state transition after entering the state. Usually the failures are due to unexpected issues with
     * DDB which will be transitional and will recover on a retry.
     * @param nextMigrationClientVersionState the state to transition to
     * @throws DependencyException If entering fails during state machine initialization.
     */
    private void enter(final MigrationClientVersionState nextMigrationClientVersionState) throws DependencyException {
        boolean success = false;
        while (!success) {
            try {
                // Enter should never fail unless it is the starting state and fails to create the GSI,
                // in which case it is an unrecoverable error that is bubbled up and KCL start up will fail.
                nextMigrationClientVersionState.enter(currentMigrationClientVersionState.clientVersion());

                currentMigrationClientVersionState = nextMigrationClientVersionState;
                log.info("Successfully transitioned to {}", nextMigrationClientVersionState.clientVersion());
                if (currentMigrationClientVersionState.clientVersion() == ClientVersion.CLIENT_VERSION_3X) {
                    terminate();
                }
                success = true;
            } catch (final DependencyException e) {
                if (currentMigrationClientVersionState.clientVersion() == ClientVersion.CLIENT_VERSION_INIT) {
                    throw e;
                }
                log.info(
                        "Transitioning from {} to {} failed, retrying after 1 second",
                        currentMigrationClientVersionState.clientVersion(),
                        nextMigrationClientVersionState.clientVersion(),
                        e);

                final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, METRICS_OPERATION);
                scope.addData(FAULT_METRIC, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
                MetricsUtil.endScope(scope);

                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ie) {
                    log.info("Interrupted while sleeping before retrying state machine transition", ie);
                }
            }
        }
    }

    private MigrationClientVersionState createMigrationClientVersionState(
            final ClientVersion clientVersion, final MigrationState migrationState) {
        switch (clientVersion) {
            case CLIENT_VERSION_2X:
                return new MigrationClientVersion2xState(
                        this, coordinatorStateDAO, stateMachineThreadPool, initializer, random);
            case CLIENT_VERSION_UPGRADE_FROM_2X:
                return new MigrationClientVersionUpgradeFrom2xState(
                        this,
                        timeProvider,
                        coordinatorStateDAO,
                        stateMachineThreadPool,
                        initializer,
                        random,
                        migrationState,
                        flipTo3XStabilizerTimeInSeconds);
            case CLIENT_VERSION_3X_WITH_ROLLBACK:
                return new MigrationClientVersion3xWithRollbackState(
                        this, coordinatorStateDAO, stateMachineThreadPool, initializer, random);
            case CLIENT_VERSION_3X:
                return new MigrationClientVersion3xState(this, initializer);
        }
        throw new IllegalStateException(String.format("Unknown client version %s", clientVersion));
    }

    public ClientVersion getCurrentClientVersion() {
        if (currentMigrationClientVersionState != null) {
            return currentMigrationClientVersionState.clientVersion();
        } else if (terminated) {
            return ClientVersion.CLIENT_VERSION_3X;
        }
        throw new UnsupportedOperationException(
                "No current state when state machine is either not initialized" + " or already terminated");
    }
}
