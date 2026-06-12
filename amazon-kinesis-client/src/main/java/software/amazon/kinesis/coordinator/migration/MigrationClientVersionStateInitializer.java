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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;

import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_2X;
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_3X;
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK;
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_INIT;
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X;
import static software.amazon.kinesis.coordinator.migration.MigrationState.MIGRATION_HASH_KEY;

/**
 * Initializer to determine start state of the state machine which identifies the
 * state to initialize KCL when it is starting up. The initial state is determined based on the
 * customer configured {@link ClientVersionConfig} and the current {@link MigrationState} in DDB,
 * as follows:
 *
 * ClientVersionConfig      | MigrationState (DDB)             | initial client version
 * -------------------------+---------------------------------+----------------------------------------------
 * COMPATIBLE_WITH_2X_PHASE1| Does not exist                  | CLIENT_VERSION_INIT (local only, not written to DDB)
 * COMPATIBLE_WITH_2X       | Does not exist                  | CLIENT_VERSION_UPGRADE_FROM_2X
 * 3X                       | Does not exist                  | CLIENT_VERSION_3X
 * COMPATIBLE_WITH_2X_PHASE1| CLIENT_VERSION_3X_WITH_ROLLBACK | CLIENT_VERSION_3X_WITH_ROLLBACK
 * COMPATIBLE_WITH_2X       | CLIENT_VERSION_3X_WITH_ROLLBACK | CLIENT_VERSION_3X_WITH_ROLLBACK
 * 3X                       | CLIENT_VERSION_3X_WITH_ROLLBACK | CLIENT_VERSION_3X
 * any                      | CLIENT_VERSION_2X               | CLIENT_VERSION_2X
 * any                      | CLIENT_VERSION_UPGRADE_FROM_2X  | CLIENT_VERSION_UPGRADE_FROM_2X
 * any                      | CLIENT_VERSION_3X               | CLIENT_VERSION_3X
 *
 * Phase 1 COMPATIBLE_WITH_2X_PHASE1 does not write any state to DDB.
 * When CLIENT_VERSION_INIT is returned, the migration state machine remains inactive and
 * KCL operates in pure 2.x
 */
@KinesisClientInternalApi
@RequiredArgsConstructor
@Slf4j
@ThreadSafe
public class MigrationClientVersionStateInitializer {
    private static final int MAX_INITIALIZATION_RETRY = 1;
    private static final long INITIALIZATION_RETRY_DELAY_MILLIS = 1000L;
    /**
     * A jitter factor of 10% to stagger the retries.
     */
    private static final double JITTER_FACTOR = 0.1;

    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ClientVersionConfig clientVersionConfig;
    private final Random random;
    private final String workerIdentifier;

    /**
     * Determine the initial client version by reading the current state from DDB and
     * applying the configured client version policy. This is a READ-ONLY operation —
     * no state is written to DDB. The DDB write happens in the state's {@code enter()} method.
     *
     * <p>If the read fails, Scheduler's retry loop will re-invoke the entire initialization.</p>
     *
     * @return the initial client version and the migration state read from DDB
     * @throws DependencyException if reading from DDB fails
     */
    public SimpleEntry<ClientVersion, MigrationState> getInitialState() throws DependencyException {
        log.info("Initializing migration state machine starting state, configured version {}", clientVersionConfig);

        try {
            final MigrationState migrationState = getMigrationStateFromDynamo();
            final ClientVersion initialClientVersion = getClientVersionForInitialization(migrationState);
            log.info("Determined initial client version {} from DDB state {}", initialClientVersion, migrationState);
            return new SimpleEntry<>(initialClientVersion, migrationState);
        } catch (final InvalidStateException e) {
            log.error("Unable to initialize state machine", e);
            throw new DependencyException(
                    new RuntimeException("Unable to determine initial state for migration state machine", e));
        }
    }

    public ClientVersion getClientVersionForInitialization(final MigrationState migrationState) {
        final ClientVersion nextClientVersion;
        switch (migrationState.getClientVersion()) {
            case CLIENT_VERSION_INIT:
                // There is no state in DDB, set state to config version and transition to configured version.
                nextClientVersion = getNextClientVersionBasedOnConfigVersion();
                log.info("Application is starting in {}", nextClientVersion);
                break;
            case CLIENT_VERSION_3X_WITH_ROLLBACK:
                if (clientVersionConfig == ClientVersionConfig.CLIENT_VERSION_CONFIG_3X) {
                    // upgrade successful, allow transition to 3x.
                    log.info("Application has successfully upgraded, transitioning to {}", CLIENT_VERSION_3X);
                    nextClientVersion = CLIENT_VERSION_3X;
                    break;
                }
                log.info("Initialize with {}", CLIENT_VERSION_3X_WITH_ROLLBACK);
                nextClientVersion = migrationState.getClientVersion();
                break;
            case CLIENT_VERSION_2X:
                log.info("Application has rolled-back, initialize with {}", CLIENT_VERSION_2X);
                nextClientVersion = migrationState.getClientVersion();
                break;
            case CLIENT_VERSION_UPGRADE_FROM_2X:
                log.info("Application is upgrading, initialize with {}", CLIENT_VERSION_UPGRADE_FROM_2X);
                nextClientVersion = migrationState.getClientVersion();
                break;
            case CLIENT_VERSION_3X:
                log.info("Initialize with {}", CLIENT_VERSION_3X);
                nextClientVersion = migrationState.getClientVersion();
                break;
            default:
                throw new IllegalStateException(String.format("Unknown version in DDB %s", migrationState));
        }
        return nextClientVersion;
    }

    private ClientVersion getNextClientVersionBasedOnConfigVersion() {
        switch (clientVersionConfig) {
            case CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1:
                // Phase 1: Remain in INIT for phase1
                // KCL operates in 2.x compatible mode without activating migration.
                return CLIENT_VERSION_INIT;
            case CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X:
                return CLIENT_VERSION_UPGRADE_FROM_2X;
            case CLIENT_VERSION_CONFIG_3X:
                return CLIENT_VERSION_3X;
        }
        throw new IllegalStateException(String.format("Unknown configured Client version %s", clientVersionConfig));
    }

    /**
     * Read the current {@link MigrationState} from DDB with retries.
     * @return current Migration state from DDB, if none exists, an initial Migration State with CLIENT_VERSION_INIT
     *          will be returned
     * @throws InvalidStateException, this occurs when dynamo table does not exist in which retrying is not useful.
     */
    private MigrationState getMigrationStateFromDynamo() throws InvalidStateException {
        return executeCallableWithRetryAndJitter(
                () -> {
                    final CoordinatorState state = coordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY);
                    if (state == null) {
                        log.info("No Migration state available in DDB");
                        return new MigrationState(workerIdentifier);
                    }
                    if (state instanceof MigrationState) {
                        log.info("Current migration state in DDB {}", state);
                        return (MigrationState) state;
                    }
                    throw new InvalidStateException(
                            String.format("Unexpected state found not confirming to MigrationState schema %s", state));
                },
                "get MigrationState from DDB");
    }

    /**
     * Helper method to retry a given callable upto MAX_INITIALIZATION_RETRY times for all retryable exceptions.
     * It considers InvalidStateException as non-retryable exception. During retry, it will compute a delay
     * with jitter before retrying.
     * @param callable  callable to invoke either until it succeeds or max retry attempts exceed.
     * @param description   a meaningful description to log exceptions
     * @return  the value returned by the callable
     * @param <T>   Return type of the callable
     * @throws InvalidStateException    If the callable throws InvalidStateException, it will not be retried and will
     *                                  be thrown back.
     */
    private <T> T executeCallableWithRetryAndJitter(final Callable<T> callable, final String description)
            throws InvalidStateException {
        int retryCount = 0;
        while (retryCount++ < MAX_INITIALIZATION_RETRY) {
            try {
                return callable.call();
            } catch (final Exception e) {
                if (e instanceof InvalidStateException) {
                    // throw the non-retryable exception
                    throw (InvalidStateException) e;
                }
                final long delay = getInitializationRetryDelay();
                log.warn("Failed to {}, retry after delay {}", description, delay, e);

                safeSleep(delay);
            }
        }
        throw new RuntimeException(
                String.format("Failed to %s after %d retries, giving up", description, MAX_INITIALIZATION_RETRY));
    }

    private void safeSleep(final long delay) {
        try {
            Thread.sleep(delay);
        } catch (final InterruptedException ie) {
            log.debug("Interrupted sleep during state machine initialization retry");
        }
    }

    /**
     * Generate a delay with jitter that is factor of the interval.
     * @return  delay with jitter
     */
    private long getInitializationRetryDelay() {
        final long jitter = (long) (random.nextDouble() * JITTER_FACTOR * INITIALIZATION_RETRY_DELAY_MILLIS);
        return INITIALIZATION_RETRY_DELAY_MILLIS + jitter;
    }
}
