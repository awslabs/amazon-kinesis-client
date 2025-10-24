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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;

import static java.util.Objects.nonNull;

/**
 * LeaseCountBasedLeaseAssignmentDecider implements KCL v2-style lease assignment logic.
 *
 * This implementation replicates the exact lease assignment behavior from KCL v2's DynamoDBLeaseTaker,
 * providing lease count-based balancing as an alternative to KCL v3's worker utilization-based approach.
 *
 * KCL v2 Assignment Strategy:
 * 1. Priority assignment of very old leases (expired for extended period)
 * 2. Calculate target leases per worker using simple division: totalLeases / totalWorkers
 * 3. Assign expired/unassigned leases round-robin to available workers
 * 4. Balance load by stealing leases from most loaded worker to underloaded workers
 *
 * Key Differences from KCL v3:
 * - Uses lease count only (ignores CPU, memory, throughput metrics)
 * - Simple target calculation vs. complex variance-based algorithms
 * - Deterministic stealing from single most loaded worker
 * - No dampening or gradual rebalancing
 *
 * Extracted Logic from DynamoDBLeaseTaker:
 * - computeLeasesToTake() → assignExpiredOrUnassignedLeases() + balanceWorkerVariance()
 * - chooseLeasesToSteal() → chooseLeasesToSteal() helper method
 * - computeLeaseCounts() → computeLeaseCounts() helper method
 */
@Slf4j
@KinesisClientInternalApi
public final class LeaseCountBasedLeaseAssignmentDecider implements LeaseAssignmentDecider {

    private final LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView;
    private final int maxLeasesForWorker;
    private final int maxLeasesToStealAtOneTime;
    private final boolean enablePriorityLeaseAssignment;
    private final int veryOldLeaseDurationNanosMultiplier;
    private final long leaseDurationNanos;

    public LeaseCountBasedLeaseAssignmentDecider(
            final LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView,
            final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime) {
        this(inMemoryStorageView, maxLeasesForWorker, maxLeasesToStealAtOneTime, true, 3, 10000L);
    }

    public LeaseCountBasedLeaseAssignmentDecider(
            final LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView,
            final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime,
            final boolean enablePriorityLeaseAssignment,
            final int veryOldLeaseDurationNanosMultiplier,
            final long leaseDurationMillis) {
        this.inMemoryStorageView = inMemoryStorageView;
        this.maxLeasesForWorker = maxLeasesForWorker;
        this.maxLeasesToStealAtOneTime = maxLeasesToStealAtOneTime;
        this.enablePriorityLeaseAssignment = enablePriorityLeaseAssignment;
        this.veryOldLeaseDurationNanosMultiplier = veryOldLeaseDurationNanosMultiplier;
        this.leaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
    }

    /**
     * Assigns expired or unassigned leases to available workers using round-robin distribution.
     *
     * This method replicates the expired lease assignment portion of DynamoDBLeaseTaker.computeLeasesToTake().
     *
     * IMPORTANT: This method handles two distinct scenarios:
     * 1. Initial assignment of expired/unassigned leases (normal flow)
     * 2. Priority assignment of very old leases (when enablePriorityLeaseAssignment is true)
     *
     * For very old leases, the assignment bypasses normal target calculations and assigns
     * up to maxLeasesForWorker leases to workers that need them.
     *
     * @param expiredOrUnAssignedLeases List of leases that need to be assigned to workers
     */
    @Override
    public void assignExpiredOrUnassignedLeases(final List<Lease> expiredOrUnAssignedLeases) {
        if (expiredOrUnAssignedLeases.isEmpty()) {
            return;
        }

        // First, check for very old leases that need priority assignment
        if (enablePriorityLeaseAssignment && handleVeryOldLeases(expiredOrUnAssignedLeases)) {
            // If we handled very old leases, we're done with assignment for this round
            return;
        }

        // Normal assignment flow for regular expired/unassigned leases
        // KCL v2 behavior: Sort by lastCounterIncrementNanos to prioritize older expired leases
        // Unassigned leases have lastCounterIncrementNanos=0 and are assigned first
        Collections.sort(
                expiredOrUnAssignedLeases,
                (l1, l2) -> Long.compare(l1.lastCounterIncrementNanos(), l2.lastCounterIncrementNanos()));

        final Set<Lease> assignedLeases = new HashSet<>();
        final List<String> availableWorkers = getAvailableWorkersForAssignment();

        // Round-robin assignment to available workers (KCL v2 style)
        int workerIndex = 0;
        for (final Lease lease : expiredOrUnAssignedLeases) {
            if (availableWorkers.isEmpty()) {
                log.info("No workers available to assign lease {}", lease.leaseKey());
                break;
            }

            // Assign to next available worker in round-robin fashion
            final String workerToAssign = availableWorkers.get(workerIndex % availableWorkers.size());
            assignLease(lease, workerToAssign);
            assignedLeases.add(lease);

            // Remove worker from available list if they reached maxLeasesForWorker capacity
            if (getWorkerLeaseCount(workerToAssign) >= maxLeasesForWorker) {
                availableWorkers.remove(workerIndex % availableWorkers.size());
                // Reset index if we removed the current worker
                if (workerIndex >= availableWorkers.size() && !availableWorkers.isEmpty()) {
                    workerIndex = 0;
                }
            } else {
                workerIndex++;
            }
        }

        expiredOrUnAssignedLeases.removeAll(assignedLeases);
    }

    /**
     * Handle very old leases with priority assignment (KCL v2 behavior).
     *
     * This replicates the priority lease assignment logic from DynamoDBLeaseTaker.computeLeasesToTake().
     * Very old leases are those that have been expired for veryOldLeaseDurationNanosMultiplier * leaseDuration.
     *
     * @param expiredOrUnAssignedLeases List of expired/unassigned leases to check
     * @return true if very old leases were found and handled, false otherwise
     */
    private boolean handleVeryOldLeases(final List<Lease> expiredOrUnAssignedLeases) {
        final long currentNanoTime = System.nanoTime();
        final long nanoThreshold = currentNanoTime - (veryOldLeaseDurationNanosMultiplier * leaseDurationNanos);

        // Find all very old leases from the entire lease list, not just expired ones
        final List<Lease> veryOldLeases = inMemoryStorageView.getLeaseList().stream()
                .filter(lease -> lease.leaseOwner() == null) // Only unowned leases
                .filter(lease -> nanoThreshold > lease.lastCounterIncrementNanos())
                .collect(Collectors.toList());

        if (veryOldLeases.isEmpty()) {
            return false;
        }

        log.info("Found {} very old leases that need priority assignment", veryOldLeases.size());

        // Shuffle to randomize assignment
        Collections.shuffle(veryOldLeases);

        // Assign very old leases to workers, respecting maxLeasesForWorker
        final Set<Lease> assignedLeases = new HashSet<>();
        for (final String workerId : inMemoryStorageView.getActiveWorkerIdSet()) {
            final int currentLeaseCount = getWorkerLeaseCount(workerId);
            final int leasesToTake =
                    Math.max(0, Math.min(maxLeasesForWorker - currentLeaseCount, veryOldLeases.size()));

            if (leasesToTake > 0) {
                final List<Lease> leasesForWorker =
                        veryOldLeases.subList(0, Math.min(leasesToTake, veryOldLeases.size()));
                for (final Lease lease : leasesForWorker) {
                    log.info(
                            "Priority assignment: assigning very old lease {} to worker {}",
                            lease.leaseKey(),
                            workerId);
                    assignLease(lease, workerId);
                    assignedLeases.add(lease);
                }
                veryOldLeases.removeAll(leasesForWorker);

                if (veryOldLeases.isEmpty()) {
                    break;
                }
            }
        }

        // Remove assigned leases from the input list
        expiredOrUnAssignedLeases.removeAll(assignedLeases);

        return !assignedLeases.isEmpty();
    }

    /**
     * Balances lease distribution across workers using KCL v2's lease count-based algorithm.
     *
     * This method replicates the load balancing portion of DynamoDBLeaseTaker.computeLeasesToTake().
     * It calculates a target lease count per worker and steals leases from overloaded workers
     * to achieve balanced distribution.
     *
     * KCL v2 Target Calculation:
     * - If workers >= leases: target = 1 (each worker gets at most 1 lease)
     * - If leases > workers: target = ceil(leases / workers)
     * - Respect maxLeasesForWorker limit
     */
    @Override
    public void balanceWorkerVariance() {
        final Map<String, Integer> leaseCounts = computeLeaseCounts();
        final int totalLeases = inMemoryStorageView.getLeaseList().size();
        final int totalWorkers = Math.max(leaseCounts.size(), 1);

        // KCL v2 target calculation logic (exact copy from DynamoDBLeaseTaker)
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

        log.info(
                "Lease count balancing: {} total leases, {} workers, target {} leases per worker",
                totalLeases,
                totalWorkers,
                target);

        // Process each worker to determine if they need to steal leases
        for (final String workerId : inMemoryStorageView.getActiveWorkerIdSet()) {
            final int myCount = leaseCounts.getOrDefault(workerId, 0);
            final int numLeasesToReachTarget = target - myCount;

            if (numLeasesToReachTarget <= 0) {
                continue;
            }

            // Check if there are any available (expired) leases first
            final List<Lease> availableLeases = inMemoryStorageView.getLeaseList().stream()
                    .filter(lease -> lease.leaseOwner() == null)
                    .collect(Collectors.toList());

            if (!availableLeases.isEmpty()) {
                // If there are available leases, we shouldn't steal
                log.debug(
                        "Worker {} needs {} leases but there are {} available leases, not stealing",
                        workerId,
                        numLeasesToReachTarget,
                        availableLeases.size());
                continue;
            }

            // No available leases, so consider stealing
            final List<Lease> leasesToSteal = chooseLeasesToSteal(leaseCounts, numLeasesToReachTarget, target);

            for (final Lease lease : leasesToSteal) {
                log.info(
                        "Worker {} needed {} leases but none were available, so it will steal lease {} from {}",
                        workerId,
                        numLeasesToReachTarget,
                        lease.leaseKey(),
                        lease.leaseOwner());
                assignLease(lease, workerId);
            }
        }
    }

    /**
     * Choose leases to steal from the most loaded worker following KCL v2 logic.
     *
     * This method is an exact copy of DynamoDBLeaseTaker.chooseLeasesToSteal() with the same
     * stealing rules and logic:
     *
     * Stealing Rules (KCL v2):
     * a) If most loaded worker has > target leases and we need >= 1: steal min(needed, excess)
     * b) If most loaded worker has == target leases and we need > 1: steal 1
     * c) Always respect maxLeasesToStealAtOneTime limit
     *
     * @param leaseCounts Map of worker ID to current lease count
     * @param needed Number of leases needed by workers below target
     * @param target Target lease count per worker
     * @return List of leases to steal from most loaded worker
     */
    private List<Lease> chooseLeasesToSteal(
            final Map<String, Integer> leaseCounts, final int needed, final int target) {
        final List<Lease> leasesToSteal = new ArrayList<>();

        // Find the most loaded worker (worker with highest lease count)
        Map.Entry<String, Integer> mostLoadedWorker = null;
        for (final Map.Entry<String, Integer> worker : leaseCounts.entrySet()) {
            if (mostLoadedWorker == null || mostLoadedWorker.getValue() < worker.getValue()) {
                mostLoadedWorker = worker;
            }
        }

        if (mostLoadedWorker == null) {
            return leasesToSteal;
        }

        // Apply KCL v2 stealing rules
        int numLeasesToSteal = 0;
        if ((mostLoadedWorker.getValue() >= target) && (needed > 0)) {
            final int leasesOverTarget = mostLoadedWorker.getValue() - target;

            // Rule a: If worker has excess leases, steal up to the excess or what's needed
            numLeasesToSteal = Math.min(needed, leasesOverTarget);

            // Rule b: If worker has exactly target leases but we need > 1, steal 1 anyway
            if ((needed > 1) && (numLeasesToSteal == 0)) {
                numLeasesToSteal = 1;
            }

            // Respect the maxLeasesToStealAtOneTime configuration limit
            numLeasesToSteal = Math.min(numLeasesToSteal, maxLeasesToStealAtOneTime);
        }

        if (numLeasesToSteal <= 0) {
            log.debug(
                    "Not stealing from most loaded worker {}. They have {}, target is {}, needed {}",
                    mostLoadedWorker.getKey(),
                    mostLoadedWorker.getValue(),
                    target,
                    needed);
            return leasesToSteal;
        }

        log.debug(
                "Will attempt to steal {} leases from most loaded worker {}. "
                        + "They have {} leases, target is {}, needed {}, maxLeasesToStealAtOneTime is {}",
                numLeasesToSteal,
                mostLoadedWorker.getKey(),
                mostLoadedWorker.getValue(),
                target,
                needed,
                maxLeasesToStealAtOneTime);

        // Collect all leases owned by the most loaded worker
        final String mostLoadedWorkerId = mostLoadedWorker.getKey();
        final List<Lease> candidates = inMemoryStorageView.getLeaseList().stream()
                .filter(lease -> mostLoadedWorkerId.equals(lease.leaseOwner()))
                .collect(Collectors.toList());

        // Randomly select leases to steal (KCL v2 behavior)
        // This prevents deterministic stealing patterns that could cause oscillation
        Collections.shuffle(candidates);
        final int toIndex = Math.min(candidates.size(), numLeasesToSteal);
        leasesToSteal.addAll(candidates.subList(0, toIndex));

        // Mark leases for stealing
        leasesToSteal.forEach(lease -> lease.isMarkedForLeaseSteal(true));

        return leasesToSteal;
    }

    /**
     * Compute lease counts per worker, replicating DynamoDBLeaseTaker.computeLeaseCounts().
     *
     * This method counts how many leases each worker currently owns, ensuring all active
     * workers are represented in the result (even those with 0 leases).
     *
     * @return Map of worker ID to current lease count
     */
    private Map<String, Integer> computeLeaseCounts() {
        final Map<String, Integer> leaseCounts = new HashMap<>();

        // Count leases per worker (only count leases with owners)
        for (final Lease lease : inMemoryStorageView.getLeaseList()) {
            if (nonNull(lease.leaseOwner())) {
                leaseCounts.merge(lease.leaseOwner(), 1, Integer::sum);
            }
        }

        // Ensure all active workers are represented, even those with 0 leases
        // This is important for target calculation and load balancing decisions
        for (final String workerId : inMemoryStorageView.getActiveWorkerIdSet()) {
            leaseCounts.putIfAbsent(workerId, 0);
        }

        return leaseCounts;
    }

    /**
     * Get list of workers available for lease assignment
     */
    private List<String> getAvailableWorkersForAssignment() {
        return inMemoryStorageView.getActiveWorkerIdSet().stream()
                .filter(workerId -> getWorkerLeaseCount(workerId) < maxLeasesForWorker)
                .collect(Collectors.toList());
    }

    /**
     * Get current lease count for a worker
     */
    private int getWorkerLeaseCount(final String workerId) {
        return (int) inMemoryStorageView.getLeaseList().stream()
                .filter(lease -> workerId.equals(lease.leaseOwner()))
                .count();
    }

    /**
     * Assign a lease to a worker
     */
    private void assignLease(final Lease lease, final String workerId) {
        log.info("Assigning lease {} to worker {}", lease.leaseKey(), workerId);
        inMemoryStorageView.performLeaseAssignment(lease, workerId);
    }
}
