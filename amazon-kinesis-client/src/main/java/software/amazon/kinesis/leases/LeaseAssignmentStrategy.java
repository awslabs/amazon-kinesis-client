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

import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Enum defining the lease assignment strategy for the KCL.
 *
 * This allows customers to choose between KCL v3's advanced worker utilization-based
 * assignment and KCL v2's simpler lease count-based assignment.
 */
@KinesisClientInternalApi
public enum LeaseAssignmentStrategy {

    /**
     * KCL v3 default strategy: Worker utilization-aware assignment.
     *
     * Uses advanced algorithms that consider worker CPU, memory, and throughput metrics
     * to make intelligent lease assignment decisions. Provides optimal load balancing
     * for heterogeneous worker fleets and varying shard throughput.
     *
     * Features:
     * - Variance-based load balancing
     * - Worker utilization metrics consideration
     * - Dampening and gradual rebalancing
     * - Throughput-aware assignment
     */
    WORKER_UTILIZATION_AWARE,

    /**
     * KCL v2 compatibility strategy: Lease count-based assignment.
     *
     * Uses simple lease count balancing where the goal is to distribute leases
     * evenly across workers based purely on lease count, ignoring worker utilization
     * metrics. Provides deterministic and predictable assignment behavior.
     *
     * Features:
     * - Simple target calculation: ceil(totalLeases / totalWorkers)
     * - Deterministic stealing from most loaded worker
     * - No utilization metrics consideration
     * - Immediate rebalancing without dampening
     */
    LEASE_COUNT_BASED
}
