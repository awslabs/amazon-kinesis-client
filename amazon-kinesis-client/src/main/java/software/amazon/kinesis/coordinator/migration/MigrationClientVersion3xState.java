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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.DynamicMigrationComponentsInitializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;

/**
 * State for CLIENT_VERSION_3X which enables KCL to run 3.x algorithms on new KCLv3.x application
 * or successfully upgraded application which upgraded from v2.x. This is a terminal state of the
 * state machine and no rollbacks are supported in this state.
 */
@KinesisClientInternalApi
@RequiredArgsConstructor
@Slf4j
@ThreadSafe
public class MigrationClientVersion3xState implements MigrationClientVersionState {
    private final MigrationStateMachine stateMachine;
    private final DynamicMigrationComponentsInitializer initializer;
    private boolean entered = false;
    private boolean left = false;

    @Override
    public ClientVersion clientVersion() {
        return ClientVersion.CLIENT_VERSION_3X;
    }

    @Override
    public synchronized void enter(final ClientVersion fromClientVersion) throws DependencyException {
        if (!entered) {
            log.info("Entering {} from {}", this, fromClientVersion);
            initializer.initializeClientVersionFor3x(fromClientVersion);
            entered = true;
        } else {
            log.info("Not entering {}", left ? "already exited state" : "already entered state");
        }
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
