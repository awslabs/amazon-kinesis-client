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

package software.amazon.kinesis.coordinator.assignment;

import java.util.List;

import software.amazon.kinesis.leases.Lease;

public interface LeaseAssignmentDecider {

    /**
     * Assigns expiredOrUnAssignedLeases to the available workers.
     */
    void assignExpiredOrUnassignedLeases(final List<Lease> expiredOrUnAssignedLeases);

    /**
     * Balances the leases between workers in the fleet.
     * Implementation can choose to balance leases based on lease count or throughput or to bring the variance in
     * resource utilization to a minimum.
     * Check documentation on implementation class to see how it balances the leases.
     */
    void balanceWorkerVariance();
}
