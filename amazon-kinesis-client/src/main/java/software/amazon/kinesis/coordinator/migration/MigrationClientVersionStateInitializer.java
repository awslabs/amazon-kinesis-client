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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_2x;
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_3x;
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_3x_WITH_ROLLBACK;
import static software.amazon.kinesis.coordinator.migration.ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2x;
import static software.amazon.kinesis.coordinator.migration.MigrationState.MIGRATION_HASH_KEY;

/**
 * Initializer to determine start state of the state machine which identifies the
 * state to initialize KCL when it is starting up. The initial state is determined based on the
 * customer configured {@link ClientVersionConfig} and the current {@link MigrationState} in DDB,
 * as follows
 * ClientVersionConfig | MigrationState (DDB)             | initial client version
 * --------------------+---------------------------------+--------------------------------
 * COMPATIBLE_WITH_2x  | Does not exist                  | CLIENT_VERSION_UPGRADE_FROM_2x
 * 3x                  | Does not exist                  | CLIENT_VERSION_3x
 * COMPATIBLE_WITH_2x  | CLIENT_VERSION_3x_WITH_ROLLBACK | CLIENT_VERSION_3x_WITH_ROLLBACK
 * 3x                  | CLIENT_VERSION_3x_WITH_ROLLBACK | CLIENT_VERSION_3x
 * any                 | CLIENT_VERSION_2x               | CLIENT_VERSION_2x
 * any                 | CLIENT_VERSION_UPGRADE_FROM_2x  | CLIENT_VERSION_UPGRADE_FROM_2x
 * any                 | CLIENT_VERSION_3x               | CLIENT_VERSION_3x
 */
@KinesisClientInternalApi
@RequiredArgsConstructor
@Slf4j
@ThreadSafe
public class MigrationClientVersionStateInitializer {
    private static final int MAX_INITIALIZATION_RETRY = 10;
    private static final long INITIALIZATION_RETRY_DELAY_MILLIS = 1000L;
    /**
     * A jitter factor of 10% to stagger the retries.
     */
    private static final double JITTER_FACTOR = 0.1;

    private final Callable<Long> timeProvider;
    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ClientVersionConfig clientVersionConfig;
    private final Random random;
    private final String workerIdentifier;

    public SimpleEntry<ClientVersion, MigrationState> getInitialState() throws DependencyException {
        log.info("Initializing migration state machine starting state, configured version {}", clientVersionConfig);

        try {
            MigrationState migrationState = getMigrationStateFromDynamo();
            int retryCount = 0;
            while (retryCount++ < MAX_INITIALIZATION_RETRY) {
                final ClientVersion initialClientVersion = getClientVersionForInitialization(migrationState);
                if (migrationState.getClientVersion() != initialClientVersion) {
                    // If update fails, the value represents current state in dynamo
                    migrationState = updateMigrationStateInDynamo(migrationState, initialClientVersion);
                    if (migrationState.getClientVersion() == initialClientVersion) {
                        // update succeeded. Transition to the state
                        return new SimpleEntry<>(initialClientVersion, migrationState);
                    }
                    final long delay = getInitializationRetryDelay();
                    log.warn(
                            "Failed to update migration state with {}, retry after delay {}",
                            initialClientVersion,
                            delay);
                    safeSleep(delay);
                } else {
                    return new SimpleEntry<>(initialClientVersion, migrationState);
                }
            }
        } catch (final InvalidStateException e) {
            log.error("Unable to initialize state machine", e);
        }
        throw new DependencyException(
                new RuntimeException("Unable to determine initial state for migration state machine"));
    }

    public ClientVersion getClientVersionForInitialization(final MigrationState migrationState) {
        final ClientVersion nextClientVersion;
        switch (migrationState.getClientVersion()) {
            case CLIENT_VERSION_INIT:
                // There is no state in DDB, set state to config version and transition to configured version.
                nextClientVersion = getNextClientVersionBasedOnConfigVersion();
                log.info("Application is starting in {}", nextClientVersion);
                break;
            case CLIENT_VERSION_3x_WITH_ROLLBACK:
                if (clientVersionConfig == ClientVersionConfig.CLIENT_VERSION_CONFIG_3x) {
                    // upgrade successful, allow transition to 3x.
                    log.info("Application has successfully upgraded, transitioning to {}", CLIENT_VERSION_3x);
                    nextClientVersion = CLIENT_VERSION_3x;
                    break;
                }
                log.info("Initialize with {}", CLIENT_VERSION_3x_WITH_ROLLBACK);
                nextClientVersion = migrationState.getClientVersion();
                break;
            case CLIENT_VERSION_2x:
                log.info("Application has rolled-back, initialize with {}", CLIENT_VERSION_2x);
                nextClientVersion = migrationState.getClientVersion();
                break;
            case CLIENT_VERSION_UPGRADE_FROM_2x:
                log.info("Application is upgrading, initialize with {}", CLIENT_VERSION_UPGRADE_FROM_2x);
                nextClientVersion = migrationState.getClientVersion();
                break;
            case CLIENT_VERSION_3x:
                log.info("Initialize with {}", CLIENT_VERSION_3x);
                nextClientVersion = migrationState.getClientVersion();
                break;
            default:
                throw new IllegalStateException(String.format("Unknown version in DDB %s", migrationState));
        }
        return nextClientVersion;
    }

    /**
     * Update the migration state's client version in dynamo conditional on the current client version
     * in dynamo. So that if another worker updates the value first, the update fails. If the update fails,
     * the method will read the latest value and return so that initialization can be retried.
     * If the value does not exist in dynamo, it will creat it.
     */
    private MigrationState updateMigrationStateInDynamo(
            final MigrationState migrationState, final ClientVersion nextClientVersion) throws InvalidStateException {
        try {
            if (migrationState.getClientVersion() == ClientVersion.CLIENT_VERSION_INIT) {
                migrationState.update(nextClientVersion, workerIdentifier);
                log.info("Creating {}", migrationState);
                final boolean created = coordinatorStateDAO.createCoordinatorStateIfNotExists(migrationState);
                if (!created) {
                    log.debug("Create {} did not succeed", migrationState);
                    return getMigrationStateFromDynamo();
                }
            } else {
                log.info("Updating {} with {}", migrationState, nextClientVersion);
                final Map<String, ExpectedAttributeValue> expectations =
                        migrationState.getDynamoClientVersionExpectation();
                migrationState.update(nextClientVersion, workerIdentifier);
                final boolean updated =
                        coordinatorStateDAO.updateCoordinatorStateWithExpectation(migrationState, expectations);
                if (!updated) {
                    log.debug("Update {} did not succeed", migrationState);
                    return getMigrationStateFromDynamo();
                }
            }
            return migrationState;
        } catch (final ProvisionedThroughputException | DependencyException e) {
            log.debug(
                    "Failed to update migration state {} with {}, return previous value to trigger a retry",
                    migrationState,
                    nextClientVersion,
                    e);
            return migrationState;
        }
    }

    private ClientVersion getNextClientVersionBasedOnConfigVersion() {
        switch (clientVersionConfig) {
            case CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2x:
                return CLIENT_VERSION_UPGRADE_FROM_2x;
            case CLIENT_VERSION_CONFIG_3x:
                return CLIENT_VERSION_3x;
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
                        return new MigrationState(MIGRATION_HASH_KEY, workerIdentifier);
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
