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

import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;

/**
 * Abstract base class for {@link MigrationClientVersionState}
 *
 * <p>This class implements the template method pattern for {@link #enter(ClientVersion)}:
 * <ol>
 *   <li>If the DDB state's clientVersion does not match this state's {@link #clientVersion()},
 *       creates or updates the DDB entry to reflect this state's version</li>
 *   <li>Delegates to {@link #doEnter(ClientVersion)} for state-specific initialization</li>
 * </ol>
 *
 * <p>Exception handling: All exceptions thrown during DDB operations
 * ({@code ProvisionedThroughputException}, {@code InvalidStateException}, {@code DependencyException})
 * are wrapped into {@link DependencyException} and thrown back to the caller. The caller
 * (either {@code MigrationStateMachineImpl.initialize()} which lets Scheduler retry, or
 * {@code MigrationStateMachineImpl.enter()} which retries indefinitely) handles the retry logic.
 *
 * <p>{@link MigrationClientVersionInitState} does NOT extend this class since it does not
 * write to DDB.
 */
@KinesisClientInternalApi
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractMigrationClientVersionState implements MigrationClientVersionState {

    protected final MigrationState curentMigrationState;
    protected final CoordinatorStateDAO coordinatorStateDAO;
    private final String workerIdentifier;

    protected boolean entered = false;
    protected boolean left = false;

    /**
     * Template method that ensures DDB state matches this state's client version before
     * delegating to the state-specific {@link #doEnter(ClientVersion)} logic.
     *
     * <p>This method is idempotent — it only executes once per state instance.
     *
     * @param fromClientVersion the client version of the previous state
     * @throws DependencyException if DDB operations fail
     */
    @Override
    public final synchronized void enter(final ClientVersion fromClientVersion) throws DependencyException {
        if (!entered) {
            log.info("Entering {} from {}", this, fromClientVersion);
            ensureDdbStateMatchesClientVersion();
            doEnter(fromClientVersion);
            entered = true;
        } else {
            log.info("Not entering {}", left ? "already exited state" : "already entered state");
        }
    }

    /**
     * State-specific enter logic. Called after DDB state has been verified/updated.
     *
     * @param fromClientVersion the client version of the previous state
     * @throws DependencyException if state-specific initialization fails
     */
    protected abstract void doEnter(ClientVersion fromClientVersion) throws DependencyException;

    /**
     * Ensures the DDB migration state matches this state's client version.
     * If the current DDB state has a different client version, this method will
     * create or update the DDB entry accordingly.
     *
     * @throws DependencyException if DDB operations fail or the state cannot be updated
     */
    private void ensureDdbStateMatchesClientVersion() throws DependencyException {
        try {
            final ClientVersion nextClientVersion = clientVersion();
            if (curentMigrationState.getClientVersion() == nextClientVersion) {
                log.info("DDB migration state already matches {}, skipping update", nextClientVersion);
                return;
            }
            final MigrationState previousMigrationState = curentMigrationState.copy();
            curentMigrationState.update(nextClientVersion, workerIdentifier);
            if (previousMigrationState.getClientVersion() == ClientVersion.CLIENT_VERSION_INIT) {
                log.info("Creating {}", curentMigrationState);
                final boolean created = coordinatorStateDAO.createCoordinatorStateIfNotExists(curentMigrationState);
                if (!created) {
                    throw new DependencyException(new RuntimeException("Failed to create migration state for "
                            + nextClientVersion + ", current DDB state: " + previousMigrationState));
                }
            } else {
                log.info("Updating {} with {}", previousMigrationState, nextClientVersion);
                final Map<String, ExpectedAttributeValue> expectations =
                        previousMigrationState.getDynamoClientVersionExpectation();
                final boolean updated =
                        coordinatorStateDAO.updateCoordinatorStateWithExpectation(curentMigrationState, expectations);
                if (!updated) {
                    throw new DependencyException(new RuntimeException("Failed to update migration state to "
                            + nextClientVersion + ", current DDB state: " + previousMigrationState));
                }
            }
        } catch (final DependencyException e) {
            throw e;
        } catch (final Exception e) {
            throw new DependencyException("Failed to transition DDB state to " + clientVersion(), e);
        }
    }
}
