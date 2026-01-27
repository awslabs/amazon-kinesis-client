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

package software.amazon.kinesis.leases;

/**
 * Enum defining the lease assignment strategy for the KCL.
 */
public enum LeaseAssignmentStrategy {

    /**
     * KCL v3 default strategy: Worker utilization-aware assignment.
     * <p>
     * Uses advanced algorithms that consider worker CPU, memory, and throughput metrics
     * to make intelligent lease assignment decisions. Provides optimal load balancing
     * for heterogeneous worker fleets and varying shard throughput.
     * <p>
     * Features:
     * - Variance-based load balancing
     * - Worker utilization metrics consideration
     * - Dampening and gradual rebalancing
     * - Throughput-aware assignment
     */
    WORKER_UTILIZATION_AWARE,

    /**
     * Lease count-based assignment.
     * <p>
     * Uses simple lease count balancing where the goal is to distribute leases
     * evenly across workers based purely on lease count, ignoring worker utilization
     * metrics.
     */
    LEASE_COUNT_BASED
}
