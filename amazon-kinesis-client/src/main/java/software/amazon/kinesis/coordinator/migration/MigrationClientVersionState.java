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

/**
 * Interface of a state implementation for the MigrationStateMachine
 */
public interface MigrationClientVersionState {

    /**
     * The associated clientVersion this state corresponds to
     * @return  ClientVersion that this state implements the logic for.
     */
    ClientVersion clientVersion();

    /**
     * Enter the state and perform the business logic of being in this state
     * which includes performing any monitoring that allows the next state
     * transition and also initializing the KCL based on the ClientVersion.
     * @param fromClientVersion from previous state if any specific action must
     *                      be taken based on the state from which this state
     *                      is being entered from.
     * @throws DependencyException if DDB fails in unexpected ways for those states
     *  that create the GSI
     */
    void enter(ClientVersion fromClientVersion) throws DependencyException;

    /**
     * Invoked after the transition to another state has occurred
     * to allow printing any helpful logs or performing cleanup.
     */
    void leave();
}
