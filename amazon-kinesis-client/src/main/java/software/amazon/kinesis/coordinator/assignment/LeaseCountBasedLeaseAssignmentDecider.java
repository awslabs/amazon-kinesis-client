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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;

import static java.util.Objects.nonNull;

/**
 * A lease assignment decider that distributes leases based on lease count balancing.
 * This implementation ensures fair distribution of leases across workers by:
 * <ul>
 *   <li>Assigning expired/unassigned leases to workers with the fewest current leases</li>
 *   <li>Balancing lease distribution to minimize variance between workers</li>
 *   <li>Respecting the maximum lease limit per worker</li>
 * </ul>
 */
@Slf4j
@KinesisClientInternalApi
@RequiredArgsConstructor
public final class LeaseCountBasedLeaseAssignmentDecider implements LeaseAssignmentDecider {

    private final LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView;
    private final int maxLeasesForWorker;

    /**
     * Assigns expired or unassigned leases to workers with the fewest current leases.
     * Leases are sorted by lastCounterIncrementNanos to ensure consistent assignment order.
     *
     * @param expiredOrUnAssignedLeases list of leases that need to be assigned to workers
     */
    @Override
    public void assignExpiredOrUnassignedLeases(final List<Lease> expiredOrUnAssignedLeases) {
        if (expiredOrUnAssignedLeases.isEmpty()
                || inMemoryStorageView.getActiveWorkerIdSet().isEmpty()) {
            return;
        }

        expiredOrUnAssignedLeases.sort(Comparator.comparingLong(Lease::lastCounterIncrementNanos));

        final Map<String, Integer> leaseCounts = computeEstimatedLeaseCounts();
        final int target = calculateTargetLeaseCount();

        log.info(
                "Lease count balancing: {} total leases, {} workers, target {} leases per worker",
                inMemoryStorageView.getLeaseList().size(),
                inMemoryStorageView.getActiveWorkerIdSet().size(),
                target);

        final List<String> workerQueue = leaseCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        final Set<Lease> assignedLeases = new HashSet<>();
        int workerIndex = 0;

        for (final Lease lease : expiredOrUnAssignedLeases) {
            if (workerIndex >= workerQueue.size()) {
                break;
            }

            final String workerToAssign = workerQueue.get(workerIndex);
            assignLease(lease, workerToAssign);
            assignedLeases.add(lease);

            final int newCount = leaseCounts.get(workerToAssign) + 1;
            leaseCounts.put(workerToAssign, newCount);

            if (newCount >= target) {
                workerIndex++;
            }
        }

        expiredOrUnAssignedLeases.removeAll(assignedLeases);
    }

    /**
     * Balances lease distribution across workers to minimize variance.
     * Avoids stealing leases that are pending handoff to prevent disruption.
     */
    @Override
    public void balanceWorkerVariance() {
        if (inMemoryStorageView.getActiveWorkerIdSet().isEmpty()) {
            return;
        }
        final Map<String, Integer> estimatedLeaseCounts = computeEstimatedLeaseCounts();
        final Map<String, List<Lease>> availableLeasesByWorker = computeAvailableLeases();
        final int target = calculateTargetLeaseCount();

        final List<Map.Entry<String, Integer>> sortedWorkers = estimatedLeaseCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toList());

        // Two pointers: left (underloaded) and right (overloaded)
        int left = 0;
        int right = sortedWorkers.size() - 1;

        while (left < right) {
            final Map.Entry<String, Integer> underloaded = sortedWorkers.get(left);
            final Map.Entry<String, Integer> overloaded = sortedWorkers.get(right);

            final int needed = target - underloaded.getValue();
            final int excess = overloaded.getValue() - target;

            if (needed <= 0) {
                left++;
                continue;
            }
            if (excess <= 0) {
                right--;
                continue;
            }

            // Find available lease from overloaded worker. These are leases not pending handoffs
            final List<Lease> availableLeases = availableLeasesByWorker.get(overloaded.getKey());
            final Lease leaseToSteal =
                    (availableLeases != null && !availableLeases.isEmpty()) ? availableLeases.remove(0) : null;

            if (leaseToSteal != null) {
                assignLease(leaseToSteal, underloaded.getKey());
                underloaded.setValue(underloaded.getValue() + 1);
                overloaded.setValue(overloaded.getValue() - 1);
            } else {
                right--;
            }
        }
    }

    /**
     * Calculates the target number of leases each worker should have.
     *
     * @return target lease count per worker, capped by maxLeasesForWorker
     */
    private int calculateTargetLeaseCount() {
        final int totalLeases = inMemoryStorageView.getLeaseList().size();
        final int totalWorkers = inMemoryStorageView.getActiveWorkerIdSet().size();
        int target;
        int leaseSpillover = 0;
        if (totalWorkers >= totalLeases) {
            // If we have n leases and n or more workers, each worker can have up to 1 lease
            target = 1;
        } else {
            // More leases than workers: target = ceil(totalLeases / totalWorkers)
            // This ensures even distribution with remainder leases going to some workers
            target = totalLeases / totalWorkers + (totalLeases % totalWorkers == 0 ? 0 : 1);

            // Spill over is the number of leases this worker should have claimed, but did not because it would
            // exceed the max allowed for this worker.
            leaseSpillover = Math.max(0, target - maxLeasesForWorker);
            if (target > maxLeasesForWorker) {
                log.warn(
                        "Target is {} leases and maxLeasesForWorker is {}. Resetting target to {},"
                                + " lease spillover is {}. Note that some shards may not be processed if no other "
                                + "workers are able to pick them up.",
                        target,
                        maxLeasesForWorker,
                        maxLeasesForWorker,
                        leaseSpillover);
                target = maxLeasesForWorker;
            }
        }
        return target;
    }

    /**
     * Computes estimated lease counts using the in-memory worker-to-leases mapping.
     * This reflects real-time assignments made during the current run.
     *
     * @return map of worker ID to estimated lease count after pending transfers
     */
    private Map<String, Integer> computeEstimatedLeaseCounts() {
        final Map<String, Integer> estimatedCounts = new HashMap<>();

        // Count leases by actual owner using workerToLeasesMap (reflects current assignments)
        for (final Map.Entry<String, Set<Lease>> entry :
                inMemoryStorageView.getWorkerToLeasesMap().entrySet()) {
            final String workerId = entry.getKey();
            final int leaseCount = entry.getValue().size();
            if (nonNull(workerId)) {
                estimatedCounts.put(workerId, leaseCount);
            }
        }

        // Ensure all active workers are represented
        for (final String workerId : inMemoryStorageView.getActiveWorkerIdSet()) {
            estimatedCounts.putIfAbsent(workerId, 0);
        }

        return estimatedCounts;
    }

    /**
     * Computes available leases that can be safely transferred (not pending handoff).
     * Uses workerToLeasesMap for real-time view of assignments.
     *
     * @return map of worker ID to list of available leases
     */
    private Map<String, List<Lease>> computeAvailableLeases() {
        final Map<String, List<Lease>> availableLeases = new HashMap<>();

        for (final Map.Entry<String, Set<Lease>> entry :
                inMemoryStorageView.getWorkerToLeasesMap().entrySet()) {
            final String workerId = entry.getKey();
            final List<Lease> workerAvailableLeases = entry.getValue().stream()
                    .filter(lease -> !lease.shutdownRequested()) // Avoid leases pending handoff
                    .collect(Collectors.toList());

            if (!workerAvailableLeases.isEmpty()) {
                availableLeases.put(workerId, workerAvailableLeases);
            }
        }

        return availableLeases;
    }

    private void assignLease(final Lease lease, final String workerId) {
        log.debug("Assigning lease {} to worker {}", lease.leaseKey(), workerId);
        inMemoryStorageView.performLeaseAssignment(lease, workerId);
    }
}
