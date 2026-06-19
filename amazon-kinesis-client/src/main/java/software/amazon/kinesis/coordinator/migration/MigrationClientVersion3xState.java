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

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.DynamicMigrationComponentsInitializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;

/**
 * State for CLIENT_VERSION_3X which enables KCL to run 3.x algorithms on new KCLv3.x application
 * or successfully upgraded application which upgraded from v2.x. This is a terminal state of the
 * state machine and no rollbacks are supported in this state.
 */
@KinesisClientInternalApi
@Slf4j
@ThreadSafe
public class MigrationClientVersion3xState extends AbstractMigrationClientVersionState {
    private final MigrationStateMachine stateMachine;
    private final DynamicMigrationComponentsInitializer initializer;

    public MigrationClientVersion3xState(
            final MigrationStateMachine stateMachine,
            final DynamicMigrationComponentsInitializer initializer,
            final CoordinatorStateDAO coordinatorStateDAO,
            final MigrationState migrationState,
            final String workerIdentifier) {
        super(migrationState, coordinatorStateDAO, workerIdentifier);
        this.stateMachine = stateMachine;
        this.initializer = initializer;
    }

    @Override
    public ClientVersion clientVersion() {
        return ClientVersion.CLIENT_VERSION_3X;
    }

    @Override
    protected void doEnter(final ClientVersion fromClientVersion) throws DependencyException {
        initializer.initializeClientVersionFor3x(fromClientVersion);
    }

    @Override
    public void leave() {
        if (entered && !left) {
            log.info("Leaving {}", this);
            entered = false;
            left = true;
        } else {
            log.info("Cannot leave {}", entered ? "already exited state" : "because state is not active");
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
