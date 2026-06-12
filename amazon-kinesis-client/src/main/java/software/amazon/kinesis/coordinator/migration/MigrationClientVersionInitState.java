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
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.DynamicMigrationComponentsInitializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;

/**
 * State representing the INIT / Phase 1 passive state of the migration state machine.
 *
 * In this state, KCL operates in pure 2.x compatible mode:
 * <ul>
 *   <li>Deterministic leader election (no DDB lock-based leader)</li>
 *   <li>Lease-count-based assignment (no LAM)</li>
 *   <li>No WorkerMetrics collection or reporting</li>
 *   <li>No GSI creation</li>
 *   <li>No coordinator state reads/writes to DDB</li>
 * </ul>
 *
 * This state is entered when the customer configures
 * {@link software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig#CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1}
 * and no migration state exists in DDB. It is a passive pre-step that ensures code is deployed
 * fleet-wide before Phase 2 activates migration logic.
 *
 * This state does not perform any monitoring or trigger transitions. It remains in this state
 * until the customer deploys with Phase 2 configuration.
 */
@KinesisClientInternalApi
@RequiredArgsConstructor
@Slf4j
public class MigrationClientVersionInitState implements MigrationClientVersionState {

    private final DynamicMigrationComponentsInitializer initializer;

    private boolean entered = false;

    @Override
    public ClientVersion clientVersion() {
        return ClientVersion.CLIENT_VERSION_INIT;
    }

    @Override
    public synchronized void enter(final ClientVersion fromClientVersion) throws DependencyException {
        if (!entered) {
            log.info("Entering Phase 1 INIT state - pure 2.x compatible mode");
            initializer.initializeClientVersionForPhase1();
            entered = true;
        } else {
            log.info("Already entered INIT state");
        }
    }

    @Override
    public void leave() {
        log.info("Leaving INIT state");
    }
}
