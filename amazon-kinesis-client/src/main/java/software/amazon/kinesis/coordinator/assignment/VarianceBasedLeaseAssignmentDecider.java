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

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * VarianceBasedLeaseAssignmentDecider
 * This implementation of LeaseAssignmentDecider performs lease assignment by considering the WorkerMetricStats values of workers
 * with respect to fleet level average of that WorkerMetricStats.
 * Rebalanced leases are assigned to workers which has maximum capacity to in terms of throughput to reach fleet level
 * across the WorkerMetricStats value. In case of multiple WorkerMetricStats, the capacity to reach fleet level average is determined by outlier
 * WorkerMetricStats.
 * To minimize the variance, the algorithm picks the fleet level average of the WorkerMetricStats for workers as a
 * pivot point and uses it to determine workers to take leases from and then assign to other workers.
 * The threshold for considering a worker for re-balance is configurable via
 * {@code reBalanceThreshold}. During reassignments the {@code dampeningPercentageValue} is used to achieve
 * critical dampening.
 */
@Slf4j
@KinesisClientInternalApi
public final class VarianceBasedLeaseAssignmentDecider implements LeaseAssignmentDecider {
    private final LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView;
    private final int dampeningPercentageValue;
    private final int reBalanceThreshold;
    private final boolean allowThroughputOvershoot;
    private final Map<String, Double> workerMetricsToFleetLevelAverageMap = new HashMap<>();
    private final PriorityQueue<WorkerMetricStats> assignableWorkerSortedByAvailableCapacity;
    private int targetLeasePerWorker;

    public VarianceBasedLeaseAssignmentDecider(
            final LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView,
            final int dampeningPercentageValue,
            final int reBalanceThreshold,
            final boolean allowThroughputOvershoot) {
        this.inMemoryStorageView = inMemoryStorageView;
        this.dampeningPercentageValue = dampeningPercentageValue;
        this.reBalanceThreshold = reBalanceThreshold;
        this.allowThroughputOvershoot = allowThroughputOvershoot;
        initialize();
        final Comparator<WorkerMetricStats> comparator = Comparator.comparingDouble(
                workerMetrics -> workerMetrics.computePercentageToReachAverage(workerMetricsToFleetLevelAverageMap));
        this.assignableWorkerSortedByAvailableCapacity = new PriorityQueue<>(comparator.reversed());
        this.assignableWorkerSortedByAvailableCapacity.addAll(
                getAvailableWorkersForAssignment(inMemoryStorageView.getActiveWorkerMetrics()));
    }

    private void initialize() {
        final Map<String, Double> workerMetricsNameToAverage = inMemoryStorageView.getActiveWorkerMetrics().stream()
                .flatMap(workerMetrics -> workerMetrics.getMetricStats().keySet().stream()
                        .map(workerMetricsName ->
                                new SimpleEntry<>(workerMetricsName, workerMetrics.getMetricStat(workerMetricsName))))
                .collect(Collectors.groupingBy(
                        SimpleEntry::getKey, HashMap::new, Collectors.averagingDouble(SimpleEntry::getValue)));

        workerMetricsToFleetLevelAverageMap.putAll(workerMetricsNameToAverage);

        final int totalWorkers =
                Math.max(inMemoryStorageView.getActiveWorkerMetrics().size(), 1);
        this.targetLeasePerWorker = Math.max(inMemoryStorageView.getLeaseList().size() / totalWorkers, 1);
    }

    private List<WorkerMetricStats> getAvailableWorkersForAssignment(final List<WorkerMetricStats> workerMetricsList) {
        // Workers with WorkerMetricStats running hot are also available for assignment as the goal is to balance
        // utilization
        // always (e.g., if all workers have hot WorkerMetricStats, balance the variance between them too)
        return workerMetricsList.stream()
                .filter(workerMetrics -> inMemoryStorageView.isWorkerTotalThroughputLessThanMaxThroughput(
                                workerMetrics.getWorkerId())
                        && inMemoryStorageView.isWorkerAssignedLeasesLessThanMaxLeases(workerMetrics.getWorkerId()))
                .collect(Collectors.toList());
    }

    @Override
    public void assignExpiredOrUnassignedLeases(final List<Lease> expiredOrUnAssignedLeases) {
        // Sort the expiredOrUnAssignedLeases using lastCounterIncrementNanos such that leases expired first are
        // picked first.
        // Unassigned leases have lastCounterIncrementNanos as zero and thus assigned first.
        Collections.sort(expiredOrUnAssignedLeases, Comparator.comparing(Lease::lastCounterIncrementNanos));
        final Set<Lease> assignedLeases = new HashSet<>();
        for (final Lease lease : expiredOrUnAssignedLeases) {
            final WorkerMetricStats workerToAssignLease = assignableWorkerSortedByAvailableCapacity.poll();
            if (nonNull(workerToAssignLease)) {
                assignLease(lease, workerToAssignLease);
                assignedLeases.add(lease);
            } else {
                log.info("No worker available to assign lease {}", lease.leaseKey());
                break;
            }
        }
        expiredOrUnAssignedLeases.removeAll(assignedLeases);
    }

    private List<WorkerMetricStats> getWorkersToTakeLeasesFromIfRequired(
            final List<WorkerMetricStats> currentWorkerMetrics,
            final String workerMetricsName,
            final double workerMetricsValueAvg) {
        final List<WorkerMetricStats> workerIdsAboveAverage = new ArrayList<>();

        final double upperLimit = workerMetricsValueAvg * (1.0D + (double) reBalanceThreshold / 100);
        final double lowerLimit = workerMetricsValueAvg * (1.0D - (double) reBalanceThreshold / 100);

        WorkerMetricStats mostLoadedWorker = null;

        log.info("Range for re-balance upper threshold {} and lower threshold {}", upperLimit, lowerLimit);

        boolean shouldTriggerReBalance = false;
        for (final WorkerMetricStats workerMetrics : currentWorkerMetrics) {
            final double currentWorkerMetricsValue = workerMetrics.getMetricStat(workerMetricsName);
            final boolean isCurrentWorkerMetricsAboveOperatingRange =
                    workerMetrics.isWorkerMetricAboveOperatingRange(workerMetricsName);
            /*
            If there is any worker, whose WorkerMetricStats value is between +/- reBalanceThreshold % of workerMetricsValueAvg or if
            worker's WorkerMetricStats value is above operating range trigger re-balance
             */
            if (currentWorkerMetricsValue > upperLimit
                    || currentWorkerMetricsValue < lowerLimit
                    || isCurrentWorkerMetricsAboveOperatingRange) {
                shouldTriggerReBalance = true;
            }
            // Perform re-balance on the worker if its above upperLimit or if current WorkerMetricStats is above
            // operating range.
            if (currentWorkerMetricsValue >= upperLimit || isCurrentWorkerMetricsAboveOperatingRange) {
                workerIdsAboveAverage.add(workerMetrics);
            }
            if (mostLoadedWorker == null
                    || mostLoadedWorker.getMetricStat(workerMetricsName) < currentWorkerMetricsValue) {
                mostLoadedWorker = workerMetrics;
            }
        }

        /*
        If workerIdsAboveAverage is empty that means there is no worker with WorkerMetricStats value above upperLimit so pick
        the worker with higher CPU. This can happen when there is worker with WorkerMetricStats value below lowerLimit but
        all other workers are within upperLimit.
        */
        if (workerIdsAboveAverage.isEmpty()) {
            workerIdsAboveAverage.add(mostLoadedWorker);
        }

        return shouldTriggerReBalance ? workerIdsAboveAverage : Collections.emptyList();
    }

    /**
     * Performs the balancing of the throughput assigned to workers based on the WorkerMetricsValues of worker with respect
     * to fleet level average.
     * Each WorkerMetricStats is treated independently to determine workers for re-balance computed (computed based on
     * reBalanceThreshold) are determined.
     * The magnitude of throughput to take is determined by how much worker is away from the average of that WorkerMetricStats
     * across fleet and in case of multiple WorkerMetricStats, the one with maximum magnitude of throughput is considered.
     */
    @Override
    public void balanceWorkerVariance() {
        final List<WorkerMetricStats> activeWorkerMetrics = inMemoryStorageView.getActiveWorkerMetrics();

        log.info("WorkerMetricStats to corresponding fleet level average : {}", workerMetricsToFleetLevelAverageMap);
        log.info("Active WorkerMetricStats : {}", activeWorkerMetrics);

        final Map<String, Double> workerIdToThroughputToTakeMap = new HashMap<>();
        String largestOutlierWorkerMetricsName = "";
        double maxThroughputTake = -1.0D;

        for (final Map.Entry<String, Double> workerMetricsToFleetLevelAverageEntry :
                workerMetricsToFleetLevelAverageMap.entrySet()) {
            final String workerMetricsName = workerMetricsToFleetLevelAverageEntry.getKey();

            // Filter workers that does not have current WorkerMetricStats. This is possible if application is adding a
            // new WorkerMetricStats and currently in phase of deployment.
            final List<WorkerMetricStats> currentWorkerMetrics = activeWorkerMetrics.stream()
                    .filter(workerMetrics -> workerMetrics.containsMetricStat(workerMetricsName))
                    .collect(Collectors.toList());

            final double fleetAverageForWorkerMetrics = workerMetricsToFleetLevelAverageEntry.getValue();

            final List<WorkerMetricStats> workerToTakeLeasesFrom = getWorkersToTakeLeasesFromIfRequired(
                    currentWorkerMetrics, workerMetricsName, fleetAverageForWorkerMetrics);

            final Map<String, Double> workerIdToThroughputToTakeForCurrentWorkerMetrics = new HashMap<>();
            double totalThroughputToTakeForCurrentWorkerMetrics = 0D;
            for (final WorkerMetricStats workerToTakeLease : workerToTakeLeasesFrom) {
                final double workerMetricsValueForWorker = workerToTakeLease.getMetricStat(workerMetricsName);
                // Load to take based on the difference compared to the fleet level average
                final double loadPercentageToTake =
                        (workerMetricsValueForWorker - fleetAverageForWorkerMetrics) / workerMetricsValueForWorker;
                // Dampen the load based on dampeningPercentageValue
                final double dampenedLoadPercentageToTake =
                        loadPercentageToTake * ((double) dampeningPercentageValue / 100);
                final double throughputToTake =
                        inMemoryStorageView.getTotalAssignedThroughput(workerToTakeLease.getWorkerId())
                                * dampenedLoadPercentageToTake;
                log.info(
                        "For worker : {} taking throughput : {} after dampening based on WorkerMetricStats : {}",
                        workerToTakeLease.getWorkerId(),
                        throughputToTake,
                        workerMetricsName);
                totalThroughputToTakeForCurrentWorkerMetrics += throughputToTake;
                workerIdToThroughputToTakeForCurrentWorkerMetrics.put(
                        workerToTakeLease.getWorkerId(), throughputToTake);
            }

            /*
               If totalThroughputToTakeForCurrentWorkerMetrics is more than maxThroughputTake that means this WorkerMetricStats is more
               outlier so consider this for reBalancing
            */
            if (maxThroughputTake < totalThroughputToTakeForCurrentWorkerMetrics) {
                largestOutlierWorkerMetricsName = workerMetricsName;
                workerIdToThroughputToTakeMap.clear();
                workerIdToThroughputToTakeMap.putAll(workerIdToThroughputToTakeForCurrentWorkerMetrics);
                maxThroughputTake = totalThroughputToTakeForCurrentWorkerMetrics;
            }
        }

        log.info(
                "Largest outlier WorkerMetricStats is : {} and total of {} throughput will be rebalanced",
                largestOutlierWorkerMetricsName,
                maxThroughputTake);
        log.info("Workers to throughput taken from them is : {}", workerIdToThroughputToTakeMap);

        final List<Map.Entry<String, Double>> sortedWorkerIdToThroughputToTakeEntries =
                new ArrayList<>(workerIdToThroughputToTakeMap.entrySet());
        // sort entries by values.
        Collections.sort(sortedWorkerIdToThroughputToTakeEntries, (e1, e2) -> e2.getValue()
                .compareTo(e1.getValue()));

        for (final Map.Entry<String, Double> workerIdToThroughputToTakeEntry :
                sortedWorkerIdToThroughputToTakeEntries) {
            final String workerId = workerIdToThroughputToTakeEntry.getKey();

            final double throughputToTake = workerIdToThroughputToTakeEntry.getValue();

            final Queue<Lease> leasesToTake = getLeasesToTake(workerId, throughputToTake);

            log.info(
                    "Leases taken from worker : {} are : {}",
                    workerId,
                    leasesToTake.stream().map(Lease::leaseKey).collect(Collectors.toSet()));

            for (final Lease lease : leasesToTake) {
                final WorkerMetricStats workerToAssign = assignableWorkerSortedByAvailableCapacity.poll();
                if (nonNull(workerToAssign)
                        && workerToAssign.willAnyMetricStatsGoAboveAverageUtilizationOrOperatingRange(
                                workerMetricsToFleetLevelAverageMap,
                                inMemoryStorageView.getTargetAverageThroughput(),
                                lease.throughputKBps(),
                                targetLeasePerWorker)) {
                    log.info("No worker to assign anymore in this iteration due to hitting average values");
                    break;
                }
                if (nonNull(workerToAssign)) {
                    assignLease(lease, workerToAssign);
                }
            }
        }

        printWorkerToUtilizationLog(inMemoryStorageView.getActiveWorkerMetrics());
    }

    private Queue<Lease> getLeasesToTake(final String workerId, final double throughputToTake) {
        final Set<Lease> existingLeases =
                inMemoryStorageView.getWorkerToLeasesMap().get(workerId);

        if (isNull(existingLeases) || existingLeases.isEmpty()) {
            return new ArrayDeque<>();
        }

        if (inMemoryStorageView.getTotalAssignedThroughput(workerId) == 0D) {
            // This is the case where throughput of this worker is zero and have 1 or more leases assigned.
            // Its not possible to determine leases to take based on throughput so simply take 1 lease and move on.
            return new ArrayDeque<>(new ArrayList<>(existingLeases).subList(0, 1));
        }

        return getLeasesCombiningToThroughput(workerId, throughputToTake);
    }

    private void assignLease(final Lease lease, final WorkerMetricStats workerMetrics) {
        if (nonNull(lease.actualOwner()) && lease.actualOwner().equals(workerMetrics.getWorkerId())) {
            // if a new owner and current owner are same then no assignment to do
            // put back the worker as well as no assignment is done
            assignableWorkerSortedByAvailableCapacity.add(workerMetrics);
            return;
        }
        workerMetrics.extrapolateMetricStatValuesForAddedThroughput(
                workerMetricsToFleetLevelAverageMap,
                inMemoryStorageView.getTargetAverageThroughput(),
                lease.throughputKBps(),
                targetLeasePerWorker);
        log.info("Assigning lease : {} to worker : {}", lease.leaseKey(), workerMetrics.getWorkerId());
        inMemoryStorageView.performLeaseAssignment(lease, workerMetrics.getWorkerId());
        if (inMemoryStorageView.isWorkerTotalThroughputLessThanMaxThroughput(workerMetrics.getWorkerId())
                && inMemoryStorageView.isWorkerAssignedLeasesLessThanMaxLeases(workerMetrics.getWorkerId())) {
            assignableWorkerSortedByAvailableCapacity.add(workerMetrics);
        }
    }

    private void printWorkerToUtilizationLog(final List<WorkerMetricStats> activeWorkerMetrics) {
        activeWorkerMetrics.forEach(workerMetrics -> log.info(
                "WorkerId : {} and average WorkerMetricStats data : {}",
                workerMetrics.getWorkerId(),
                workerMetrics.getMetricStatsMap()));
    }

    private Queue<Lease> getLeasesCombiningToThroughput(final String workerId, final double throughputToGet) {
        final List<Lease> assignedLeases =
                new ArrayList<>(inMemoryStorageView.getWorkerToLeasesMap().get(workerId));
        if (assignedLeases.isEmpty()) {
            // This is possible if the worker is having high utilization but does not have any leases assigned to it
            return new ArrayDeque<>();
        }
        // Shuffle leases to randomize what leases gets picked.
        Collections.shuffle(assignedLeases);
        final Queue<Lease> response = new ArrayDeque<>();
        double remainingThroughputToGet = throughputToGet;
        for (final Lease lease : assignedLeases) {
            // if adding this lease makes throughout to take go below zero avoid taking this lease.
            if (remainingThroughputToGet - lease.throughputKBps() <= 0) {
                continue;
            }
            remainingThroughputToGet -= lease.throughputKBps();
            response.add(lease);
        }

        // If allowThroughputOvershoot is set to true, take a minimum throughput lease
        if (allowThroughputOvershoot && response.isEmpty()) {
            assignedLeases.stream()
                    .min(Comparator.comparingDouble(Lease::throughputKBps))
                    .ifPresent(response::add);
        }
        return response;
    }
}
