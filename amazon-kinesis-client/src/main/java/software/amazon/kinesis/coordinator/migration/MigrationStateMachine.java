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

import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;

/**
 * State machine that provides:
 * 1. Seamless upgrade from 2.x to 3.x - 3.x has introduced new algorithms that are not compatible with 2.x
 *    workers, so the state machine allows to seamlessly run the 2.x functionality to be compliant with any
 *    2.x worker in the fleet, and also seamlessly switch to 3.x functionality when all KCL workers are
 *    3.x complaint.
 * 2. Instant rollbacks - Rollbacks are supported using the KCL Migration tool to revert back to 2.x functionality
 *    if customer finds regressions in 3.x functionality.
 * 3. Instant roll-forwards - Once any issue has been mitigated, rollfowards are supported instantly
 *    with KCL Migration tool.
 */
public interface MigrationStateMachine {
    /**
     * Initialize the state machine by identifying the initial state when the KCL worker comes up for the first time.
     * @throws DependencyException  When unable to identify the initial state.
     */
    void initialize() throws DependencyException;

    /**
     * Shutdown state machine and perform necessary cleanup for the worker to gracefully shutdown
     */
    void shutdown();

    /**
     * Terminate the state machine when it reaches a terminal state, which is a successful upgrade
     * to v3.x.
     */
    void terminate();

    /**
     * Peform transition from current state to the given new ClientVersion
     * @param nextClientVersion clientVersion of the new state the state machine must transition to
     * @param state the current MigrationState in dynamo
     * @throws InvalidStateException    when transition fails, this allows the state machine to stay
     *                                  in the current state until a valid transition is possible
     * @throws DependencyException      when transition fails due to dependency on DDB failing in
     *                                  unexpected ways.
     */
    void transitionTo(final ClientVersion nextClientVersion, final MigrationState state)
            throws InvalidStateException, DependencyException;

    /**
     * Get the ClientVersion of current state machine state.
     * @return ClientVersion of current state machine state
     */
    ClientVersion getCurrentClientVersion();
}
