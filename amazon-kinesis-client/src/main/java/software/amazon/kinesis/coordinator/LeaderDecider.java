/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.coordinator;

/**
 * Used in conjunction with periodic shard sync.
 * Implement this interface to allow KCL to decide if the current worker should execute shard sync.
 * When periodic shard sync is enabled, PeriodicShardSyncManager periodically checks if the current
 * worker is one of the leaders designated to execute shard-sync and then acts accordingly.
 */
public interface LeaderDecider {
    String METRIC_OPERATION_LEADER_DECIDER = "LeaderDecider";
    String METRIC_OPERATION_LEADER_DECIDER_IS_LEADER = METRIC_OPERATION_LEADER_DECIDER + ":IsLeader";

    /**
     * Method invoked to check the given workerId corresponds to one of the workers
     * designated to execute shard-syncs periodically.
     *
     * @param workerId ID of the worker
     * @return True if the worker with ID workerId can execute shard-sync. False otherwise.
     */
    Boolean isLeader(String workerId);

    /**
     * Can be invoked, if needed, to shutdown any clients/thread-pools
     * being used in the LeaderDecider implementation.
     */
    void shutdown();

    /**
     * Performs initialization tasks for decider if any.
     */
    default void initialize() {
        // No-op by default
    }

    /**
     * If the current worker is the leader, then releases the leadership else does nothing.
     * This might not be relevant for some implementations, for e.g. DeterministicShuffleShardSyncLeaderDecider does
     * not have mechanism to release leadership.
     *
     * Current worker if leader releases leadership, it's possible that the current worker assume leadership sometime
     * later again in future elections.
     */
    default void releaseLeadershipIfHeld() {
        // No-op by default
    }
}
