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

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.metrics.NullMetricsScope;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Performs the LeaseAssignment for the application. This starts by loading the leases and workerMetrics from the
 * storage and then starts by assignment (in-memory) of expired and/or unassigned leases after which it tries to perform
 * balancing of load among the workers by re-assign leases.
 * In the end, performs actual assignment by writing to storage.
 */
@Slf4j
@RequiredArgsConstructor
@KinesisClientInternalApi
public final class LeaseAssignmentManager {

    /**
     * Default number of continuous failure execution after which leadership is released.
     */
    private static final int DEFAULT_FAILURE_COUNT_TO_SWITCH_LEADER = 3;

    /**
     * Default multiplier for LAM frequency with respect to leaseDurationMillis (lease failover millis).
     * If leaseDurationMillis is 10000 millis, default LAM frequency is 20000 millis.
     */
    private static final int DEFAULT_LEASE_ASSIGNMENT_MANAGER_FREQ_MULTIPLIER = 2;

    /**
     * Default parallelism factor for scaling lease table.
     */
    private static final int DEFAULT_LEASE_TABLE_SCAN_PARALLELISM_FACTOR = 10;

    private static final String FORCE_LEADER_RELEASE_METRIC_NAME = "ForceLeaderRelease";

    /**
     * Default retry attempt for loading leases and workers before giving up.
     */
    private static final int DDB_LOAD_RETRY_ATTEMPT = 1;

    /**
     * Internal threadpool used to parallely perform assignment operation by calling storage.
     */
    private static final ExecutorService LEASE_ASSIGNMENT_CALL_THREAD_POOL =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private static final String METRICS_LEASE_ASSIGNMENT_MANAGER = "LeaseAssignmentManager";
    private static final String METRICS_INCOMPLETE_EXPIRED_LEASES_ASSIGNMENT =
            "LeaseAssignmentManager.IncompleteExpiredLeasesAssignment";
    public static final int DEFAULT_NO_OF_SKIP_STAT_FOR_DEAD_WORKER_THRESHOLD = 2;

    private final LeaseRefresher leaseRefresher;
    private final WorkerMetricStatsDAO workerMetricsDAO;
    private final LeaderDecider leaderDecider;
    private final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config;
    private final String currentWorkerId;
    private final Long leaseDurationMillis;
    private final MetricsFactory metricsFactory;
    private final ScheduledExecutorService executorService;
    private final Supplier<Long> nanoTimeProvider;
    private final int maxLeasesForWorker;
    private final LeaseManagementConfig.GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig;
    private boolean tookOverLeadershipInThisRun = false;
    private final Map<String, Lease> prevRunLeasesState = new HashMap<>();

    private Future<?> managerFuture;

    private int noOfContinuousFailedAttempts = 0;
    private int lamRunCounter = 0;

    public synchronized void start() {
        if (isNull(managerFuture)) {
            // LAM can be dynamically started/stopped and restarted during MigrationStateMachine execution
            // so reset the flag to refresh the state before processing during a restart of LAM.
            tookOverLeadershipInThisRun = false;
            managerFuture = executorService.scheduleWithFixedDelay(
                    this::performAssignment,
                    0L,
                    leaseDurationMillis * DEFAULT_LEASE_ASSIGNMENT_MANAGER_FREQ_MULTIPLIER,
                    TimeUnit.MILLISECONDS);
            log.info("Started LeaseAssignmentManager");
            return;
        }
        log.info("LeaseAssignmentManager already running...");
    }

    public synchronized void stop() {
        if (nonNull(managerFuture)) {
            log.info("Completed shutdown of LeaseAssignmentManager");
            managerFuture.cancel(true);
            managerFuture = null;
            return;
        }
        log.info("LeaseAssignmentManager is not running...");
    }

    /**
     * Creates the MetricsScope for given {@param operation} by calling metricsFactory and falls back to
     * NullMetricsScope if failed to create MetricsScope.
     * @param operation Operation name for MetricsScope
     * @return instance of MetricsScope
     */
    private MetricsScope createMetricsScope(final String operation) {
        try {
            return MetricsUtil.createMetricsWithOperation(metricsFactory, operation);
        } catch (final Exception e) {
            log.error("Failed to create metrics scope defaulting to no metrics.", e);
            return new NullMetricsScope();
        }
    }

    private void performAssignment() {

        final MetricsScope metricsScope = createMetricsScope(METRICS_LEASE_ASSIGNMENT_MANAGER);
        final long startTime = System.currentTimeMillis();
        boolean success = false;

        try {

            // If the current worker is not leader, then do nothing as assignment is executed on leader.
            if (!leaderDecider.isLeader(currentWorkerId)) {
                log.info("Current worker {} is not a leader, ignore", currentWorkerId);
                this.tookOverLeadershipInThisRun = false;
                success = true;
                return;
            }

            if (!this.tookOverLeadershipInThisRun) {
                // This means that there was leader change, perform cleanup of state as this is leader switch.
                this.tookOverLeadershipInThisRun = true;
                this.lamRunCounter = 0;
                prepareAfterLeaderSwitch();
            }
            log.info("Current worker {} is a leader, performing assignment", currentWorkerId);

            final InMemoryStorageView inMemoryStorageView = new InMemoryStorageView();

            final long loadStartTime = System.currentTimeMillis();
            inMemoryStorageView.loadInMemoryStorageView(metricsScope);
            MetricsUtil.addLatency(metricsScope, "LeaseAndWorkerMetricsLoad", loadStartTime, MetricsLevel.DETAILED);

            publishLeaseAndWorkerCountMetrics(metricsScope, inMemoryStorageView);
            final LeaseAssignmentDecider leaseAssignmentDecider = new VarianceBasedLeaseAssignmentDecider(
                    inMemoryStorageView,
                    config.dampeningPercentage(),
                    config.reBalanceThresholdPercentage(),
                    config.allowThroughputOvershoot());

            updateLeasesLastCounterIncrementNanosAndLeaseShutdownTimeout(
                    inMemoryStorageView.getLeaseList(), inMemoryStorageView.getLeaseTableScanTime());

            // This does not include the leases from the worker that has expired (based on WorkerMetricStats's
            // lastUpdateTime)
            // but the lease is not expired (based on the leaseCounter on lease).
            // If a worker has died, the lease will be expired and assigned in next iteration.
            final List<Lease> expiredOrUnAssignedLeases = inMemoryStorageView.getLeaseList().stream()
                    .filter(lease -> lease.isExpired(
                                    TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis),
                                    inMemoryStorageView.getLeaseTableScanTime())
                            || Objects.isNull(lease.actualOwner()))
                    // marking them for direct reassignment.
                    .map(l -> l.isExpiredOrUnassigned(true))
                    .collect(Collectors.toList());

            log.info("Total expiredOrUnassignedLeases count : {}", expiredOrUnAssignedLeases.size());
            metricsScope.addData(
                    "ExpiredLeases", expiredOrUnAssignedLeases.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);

            final long expiredAndUnassignedLeaseAssignmentStartTime = System.currentTimeMillis();
            leaseAssignmentDecider.assignExpiredOrUnassignedLeases(expiredOrUnAssignedLeases);
            MetricsUtil.addLatency(
                    metricsScope,
                    "AssignExpiredOrUnassignedLeases",
                    expiredAndUnassignedLeaseAssignmentStartTime,
                    MetricsLevel.DETAILED);

            if (!expiredOrUnAssignedLeases.isEmpty()) {
                // When expiredOrUnAssignedLeases is not empty, that means
                // that we were not able to assign all expired or unassigned leases and hit the maxThroughput
                // per worker for all workers.
                log.warn("Not able to assign all expiredOrUnAssignedLeases");
                metricsScope.addData(
                        "LeaseSpillover", expiredOrUnAssignedLeases.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
            }

            if (shouldRunVarianceBalancing()) {
                final long balanceWorkerVarianceStartTime = System.currentTimeMillis();
                final int totalNewAssignmentBeforeWorkerVarianceBalancing =
                        inMemoryStorageView.leaseToNewAssignedWorkerMap.size();
                leaseAssignmentDecider.balanceWorkerVariance();
                MetricsUtil.addLatency(
                        metricsScope, "BalanceWorkerVariance", balanceWorkerVarianceStartTime, MetricsLevel.DETAILED);
                metricsScope.addData(
                        "NumOfLeasesReassignment",
                        inMemoryStorageView.leaseToNewAssignedWorkerMap.size()
                                - totalNewAssignmentBeforeWorkerVarianceBalancing,
                        StandardUnit.COUNT,
                        MetricsLevel.SUMMARY);
            }

            if (inMemoryStorageView.leaseToNewAssignedWorkerMap.isEmpty()) {
                log.info("No new lease assignment performed in this iteration");
            }

            parallelyAssignLeases(inMemoryStorageView, metricsScope);
            printPerWorkerLeases(inMemoryStorageView);
            deleteStaleWorkerMetricsEntries(inMemoryStorageView, metricsScope);
            success = true;
            noOfContinuousFailedAttempts = 0;
        } catch (final Exception e) {
            log.error("LeaseAssignmentManager failed to perform lease assignment.", e);
            noOfContinuousFailedAttempts++;
            if (noOfContinuousFailedAttempts >= DEFAULT_FAILURE_COUNT_TO_SWITCH_LEADER) {
                log.error(
                        "Failed to perform assignment {} times in a row, releasing leadership from worker : {}",
                        DEFAULT_FAILURE_COUNT_TO_SWITCH_LEADER,
                        currentWorkerId);
                MetricsUtil.addCount(metricsScope, FORCE_LEADER_RELEASE_METRIC_NAME, 1, MetricsLevel.SUMMARY);
                leaderDecider.releaseLeadershipIfHeld();
            }
        } finally {
            MetricsUtil.addSuccessAndLatency(metricsScope, success, startTime, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(metricsScope);
        }
    }

    private boolean shouldRunVarianceBalancing() {
        final boolean response = this.lamRunCounter == 0;
        /*
        To avoid lamRunCounter grow large, keep it within [0,varianceBalancingFrequency).
        If varianceBalancingFrequency is 5 lamRunCounter value will be within 0 to 4 and method return true when
        lamRunCounter is 0.
         */
        this.lamRunCounter = (this.lamRunCounter + 1) % config.varianceBalancingFrequency();
        return response;
    }

    /**
     * Deletes the WorkerMetricStats entries which are stale(not updated since long time, ref
     * {@link LeaseAssignmentManager#isWorkerMetricsEntryStale} for the condition to evaluate staleness)
     */
    private void deleteStaleWorkerMetricsEntries(
            final InMemoryStorageView inMemoryStorageView, final MetricsScope metricsScope) {
        final long startTime = System.currentTimeMillis();
        try {
            final List<WorkerMetricStats> staleWorkerMetricsList = inMemoryStorageView.getWorkerMetricsList().stream()
                    .filter(this::isWorkerMetricsEntryStale)
                    .collect(Collectors.toList());
            MetricsUtil.addCount(
                    metricsScope, "TotalStaleWorkerMetricsEntry", staleWorkerMetricsList.size(), MetricsLevel.DETAILED);
            log.info("Number of stale workerMetrics entries : {}", staleWorkerMetricsList.size());
            log.info("Stale workerMetrics list : {}", staleWorkerMetricsList);

            final List<CompletableFuture<Boolean>> completableFutures = staleWorkerMetricsList.stream()
                    .map(workerMetrics -> CompletableFuture.supplyAsync(
                            () -> workerMetricsDAO.deleteMetrics(workerMetrics), LEASE_ASSIGNMENT_CALL_THREAD_POOL))
                    .collect(Collectors.toList());

            CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                    .join();
        } finally {
            MetricsUtil.addLatency(metricsScope, "StaleWorkerMetricsCleanup", startTime, MetricsLevel.DETAILED);
        }
    }

    /**
     * WorkerMetricStats entry is considered stale if the lastUpdateTime of the workerMetrics is older than
     * workerMetricsStalenessThreshold * workerMetricsReporterFreqInMillis.
     */
    private boolean isWorkerMetricsEntryStale(final WorkerMetricStats workerMetrics) {
        return Duration.between(Instant.ofEpochSecond(workerMetrics.getLastUpdateTime()), Instant.now())
                        .toMillis()
                > config.staleWorkerMetricsEntryCleanupDuration().toMillis();
    }

    private void printPerWorkerLeases(final InMemoryStorageView storageView) {
        storageView.getActiveWorkerIdSet().forEach(activeWorkerId -> {
            log.info(
                    "Worker : {} and total leases : {} and totalThroughput : {}",
                    activeWorkerId,
                    Optional.ofNullable(storageView.getWorkerToLeasesMap().get(activeWorkerId))
                            .orElse(Collections.EMPTY_SET)
                            .size(),
                    storageView.getWorkerToTotalAssignedThroughputMap().get(activeWorkerId));
        });
    }

    private void parallelyAssignLeases(final InMemoryStorageView inMemoryStorageView, final MetricsScope metricsScope) {
        final AtomicInteger failedAssignmentCounter = new AtomicInteger(0);
        final long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            CompletableFuture.allOf(inMemoryStorageView.getLeaseToNewAssignedWorkerMap().entrySet().stream()
                            // ignore leases that are heartbeating and pending graceful shutdown checkpoint.
                            .filter(entry -> !entry.getKey().blockedOnPendingCheckpoint(getNanoTimeMillis()))
                            .map(entry -> CompletableFuture.supplyAsync(
                                    () -> {
                                        try {
                                            final Lease lease = entry.getKey();
                                            if (gracefulLeaseHandoffConfig.isGracefulLeaseHandoffEnabled()
                                                    && lease.isEligibleForGracefulShutdown()) {
                                                return handleGracefulLeaseHandoff(
                                                        lease, entry.getValue(), failedAssignmentCounter);
                                            } else {
                                                return handleRegularLeaseAssignment(
                                                        lease, entry.getValue(), failedAssignmentCounter);
                                            }
                                        } catch (Exception e) {
                                            throw new CompletionException(e);
                                        }
                                    },
                                    LEASE_ASSIGNMENT_CALL_THREAD_POOL))
                            .toArray(CompletableFuture[]::new))
                    .join();
            success = true;
        } finally {
            MetricsUtil.addCount(
                    metricsScope, "FailedAssignmentCount", failedAssignmentCounter.get(), MetricsLevel.DETAILED);
            MetricsUtil.addSuccessAndLatency(
                    metricsScope, "ParallelyAssignLeases", success, startTime, MetricsLevel.DETAILED);
        }
    }

    private boolean handleGracefulLeaseHandoff(Lease lease, String newOwner, AtomicInteger failedAssignmentCounter)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final boolean response = leaseRefresher.initiateGracefulLeaseHandoff(lease, newOwner);
        if (response) {
            // new handoff assignment. add the timeout.
            lease.checkpointOwnerTimeoutTimestampMillis(getCheckpointOwnerTimeoutTimestampMillis());
        } else {
            failedAssignmentCounter.incrementAndGet();
        }
        return response;
    }

    private boolean handleRegularLeaseAssignment(Lease lease, String newOwner, AtomicInteger failedAssignmentCounter)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final boolean response = leaseRefresher.assignLease(lease, newOwner);
        if (response) {
            // Successful assignment updates the leaseCounter, update the nanoTime for counter update.
            lease.lastCounterIncrementNanos(nanoTimeProvider.get());
        } else {
            failedAssignmentCounter.incrementAndGet();
        }
        return response;
    }

    private void publishLeaseAndWorkerCountMetrics(
            final MetricsScope metricsScope, final InMemoryStorageView inMemoryStorageView) {
        // Names of the metrics are kept in sync with what is published in LeaseTaker.
        metricsScope.addData(
                "TotalLeases", inMemoryStorageView.leaseList.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
        metricsScope.addData(
                "NumWorkers", inMemoryStorageView.activeWorkerMetrics.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
    }

    // Method updates all new leases with currentTime if the counter is updated since last run else keeps whatever
    // was prev and update the prevRunLeasesState
    private void updateLeasesLastCounterIncrementNanosAndLeaseShutdownTimeout(
            final List<Lease> leaseList, final Long scanTime) {
        for (final Lease lease : leaseList) {
            final Lease prevLease = prevRunLeasesState.get(lease.leaseKey());

            // make sure lease shutdown timeouts are tracked.
            if (lease.shutdownRequested()) {
                // previous and current leases might have same next and checkpoint owners but there is no
                // guarantee that the latest shutdown is the same shutdown in the previous lease for example
                // some other leaders change the lease states while this worker waiting for it's LAM run.
                // This is the best effort to prevent marking the incorrect timeout.
                if (isNull(prevLease) || !prevLease.shutdownRequested() || !isSameOwners(lease, prevLease)) {
                    // Add new value if previous is null, previous lease is not shutdown pending or the owners
                    // don't match
                    lease.checkpointOwnerTimeoutTimestampMillis(getCheckpointOwnerTimeoutTimestampMillis());
                } else {
                    lease.checkpointOwnerTimeoutTimestampMillis(prevLease.checkpointOwnerTimeoutTimestampMillis());
                }
            }

            if (isNull(prevLease)) {
                lease.lastCounterIncrementNanos(
                        isNull(lease.actualOwner())
                                // This is an unassigned lease, mark as 0L that puts this in first in assignment order
                                ? 0L
                                : scanTime);
            } else {
                lease.lastCounterIncrementNanos(
                        lease.leaseCounter() > prevLease.leaseCounter()
                                ? scanTime
                                : prevLease.lastCounterIncrementNanos());
            }
        }
        prevRunLeasesState.clear();
        prevRunLeasesState.putAll(leaseList.stream().collect(Collectors.toMap(Lease::leaseKey, Function.identity())));
    }

    private void prepareAfterLeaderSwitch() {
        prevRunLeasesState.clear();
        noOfContinuousFailedAttempts = 0;
    }

    /**
     * In memory view of the leases and workerMetrics.
     * This class supports queries (e.g., leases assigned to worker or total throughout assigned to worker).
     */
    @Getter
    class InMemoryStorageView {

        // This is in-memory view of the workerToLeaseMapping, this is updated in-memory before actual
        // changes to storage.
        private final Map<String, Set<Lease>> workerToLeasesMap = new HashMap<>();
        /**
         * This is computed initially after the loading leases and then updated when the
         * {@link InMemoryStorageView#performLeaseAssignment} is called.
         */
        private final Map<String, Double> workerToTotalAssignedThroughputMap = new HashMap<>();
        /**
         * Captures the new assignment done during the lifecycle of single run.
         */
        private final Map<Lease, String> leaseToNewAssignedWorkerMap = new HashMap<>();

        /**
         * List of all leases in the application.
         */
        private List<Lease> leaseList;
        /**
         * List of workers which are active (i.e., updated metric stats before the threshold ref)
         * {@link this#computeWorkerExpiryThresholdInSecond})
         */
        private List<WorkerMetricStats> activeWorkerMetrics;
        /**
         * List of all workerMetrics entries from storage.
         */
        private List<WorkerMetricStats> workerMetricsList;
        /**
         * List of active workers ids.
         */
        private Set<String> activeWorkerIdSet;
        /**
         * Wall time in nanoseconds when the lease table scan was completed.
         */
        private long leaseTableScanTime = 0L;
        /**
         * Average throughput for all workers.
         */
        private double targetAverageThroughput;

        /**
         * Update {@ref inMemoryWorkerToLeasesMapping} with the change in ownership and update newLeaseAssignmentMap
         *
         * @param lease lease changing assignment
         * @param newOwner new owner of the lease
         */
        public void performLeaseAssignment(final Lease lease, final String newOwner) {
            final String existingOwner = lease.actualOwner();
            workerToLeasesMap.get(existingOwner).remove(lease);
            workerToLeasesMap
                    .computeIfAbsent(newOwner, owner -> new HashSet<>())
                    .add(lease);
            updateWorkerThroughput(newOwner, lease.throughputKBps());
            // Remove the same lease throughput from oldOwner
            updateWorkerThroughput(existingOwner, -lease.throughputKBps());
            leaseToNewAssignedWorkerMap.put(lease, newOwner);
        }

        /**
         * Scans the LeaseTable and WorkerMetricStats in parallel and load the data and populate datastructures used
         * in lease assignment.
         */
        public void loadInMemoryStorageView(final MetricsScope metricsScope) throws Exception {
            final CompletableFuture<Map.Entry<List<Lease>, List<String>>> leaseListFuture = loadLeaseListAsync();

            final CompletableFuture<List<WorkerMetricStats>> workerMetricsFuture = loadWorkerMetricStats();

            final List<WorkerMetricStats> workerMetricsFromStorage = workerMetricsFuture.join();

            final List<String> listOfWorkerIdOfInvalidWorkerMetricsEntry = workerMetricsFromStorage.stream()
                    .filter(workerMetrics -> !workerMetrics.isValidWorkerMetric())
                    .map(WorkerMetricStats::getWorkerId)
                    .collect(Collectors.toList());
            log.warn("List of workerIds with invalid entries : {}", listOfWorkerIdOfInvalidWorkerMetricsEntry);
            if (!listOfWorkerIdOfInvalidWorkerMetricsEntry.isEmpty()) {
                metricsScope.addData(
                        "NumWorkersWithInvalidEntry",
                        listOfWorkerIdOfInvalidWorkerMetricsEntry.size(),
                        StandardUnit.COUNT,
                        MetricsLevel.SUMMARY);
            }

            // Valid entries are considered further, for validity of entry refer WorkerMetricStats#isValidWorkerMetrics
            this.workerMetricsList = workerMetricsFromStorage.stream()
                    .filter(WorkerMetricStats::isValidWorkerMetric)
                    .collect(Collectors.toList());

            log.info("Total WorkerMetricStats available : {}", workerMetricsList.size());
            final long workerExpiryThreshold = computeWorkerExpiryThresholdInSecond();

            final long countOfWorkersWithFailingWorkerMetric = workerMetricsList.stream()
                    .filter(WorkerMetricStats::isAnyWorkerMetricFailing)
                    .count();
            if (countOfWorkersWithFailingWorkerMetric != 0) {
                metricsScope.addData(
                        "NumWorkersWithFailingWorkerMetric",
                        countOfWorkersWithFailingWorkerMetric,
                        StandardUnit.COUNT,
                        MetricsLevel.SUMMARY);
            }

            final Map.Entry<List<Lease>, List<String>> leaseListResponse = leaseListFuture.join();
            this.leaseList = leaseListResponse.getKey();
            log.warn("Leases that failed deserialization : {}", leaseListResponse.getValue());
            if (!leaseListResponse.getValue().isEmpty()) {
                MetricsUtil.addCount(
                        metricsScope,
                        "LeaseDeserializationFailureCount",
                        leaseListResponse.getValue().size(),
                        MetricsLevel.SUMMARY);
            }
            this.leaseTableScanTime = nanoTimeProvider.get();
            log.info("Total Leases available : {}", leaseList.size());

            final double averageLeaseThroughput = leaseList.stream()
                    .filter(lease -> nonNull(lease.throughputKBps()))
                    .mapToDouble(Lease::throughputKBps)
                    .average()
                    // If none of the leases has any value, that means its app
                    // startup time and thus assigns 0 in that case to start with.
                    .orElse(0D);
            /*
             * If a workerMetrics has a metric (i.e. has -1 value in last index which denotes failure),
             * skip it from activeWorkerMetrics and no new action on it will be done
             * (new assignment etc.) until the metric has non -1 value in last index. This is to avoid performing action
             * with the stale data on worker.
             */
            this.activeWorkerMetrics = workerMetricsList.stream()
                    .filter(workerMetrics -> workerMetrics.getLastUpdateTime() >= workerExpiryThreshold
                            && !workerMetrics.isAnyWorkerMetricFailing())
                    .collect(Collectors.toList());
            log.info("activeWorkerMetrics : {}", activeWorkerMetrics.size());
            targetAverageThroughput =
                    averageLeaseThroughput * leaseList.size() / Math.max(1, activeWorkerMetrics.size());
            leaseList.forEach(lease -> {
                if (isNull(lease.throughputKBps())) {
                    // If the lease is unassigned, it will not have any throughput value, use average throughput
                    // as good enough value to start with.
                    lease.throughputKBps(averageLeaseThroughput);
                }
                workerToLeasesMap
                        .computeIfAbsent(lease.actualOwner(), workerId -> new HashSet<>())
                        .add(lease);
                updateWorkerThroughput(lease.actualOwner(), lease.throughputKBps());
            });

            this.activeWorkerIdSet = new HashSet<>();
            // Calculate initial ratio
            this.activeWorkerMetrics.forEach(workerMetrics -> {
                activeWorkerIdSet.add(workerMetrics.getWorkerId());
                workerMetrics.setEmaAlpha(config.workerMetricsEMAAlpha());
                if (workerMetrics.isUsingDefaultWorkerMetric()) {
                    setOperatingRangeAndWorkerMetricsDataForDefaultWorker(
                            workerMetrics,
                            getTotalAssignedThroughput(workerMetrics.getWorkerId()) / targetAverageThroughput);
                }
            });
        }

        private void updateWorkerThroughput(final String workerId, final double leaseThroughput) {
            double value = workerToTotalAssignedThroughputMap.computeIfAbsent(workerId, worker -> (double) 0L);
            workerToTotalAssignedThroughputMap.put(workerId, value + leaseThroughput);
        }

        private void setOperatingRangeAndWorkerMetricsDataForDefaultWorker(
                final WorkerMetricStats workerMetrics, final Double ratio) {
            // for workers with default WorkerMetricStats, the operating range ceiling of 100 represents the
            // target throughput. This way, with either heterogeneous or homogeneous fleets
            // of explicit WorkerMetricStats and default WorkerMetricStats applications, load will be evenly
            // distributed.
            log.info(
                    "Worker [{}] is using default WorkerMetricStats, setting initial utilization ratio to [{}].",
                    workerMetrics.getWorkerId(),
                    ratio);
            workerMetrics.setOperatingRange(ImmutableMap.of("T", ImmutableList.of(100L)));
            workerMetrics.setMetricStats(ImmutableMap.of("T", ImmutableList.of(ratio * 100, ratio * 100)));
        }

        /**
         * Calculates the value threshold in seconds for a worker to be considered as active.
         * If a worker has not updated the WorkerMetricStats entry within this threshold, the worker is not considered
         * as active.
         *
         * @return wall time in seconds
         */
        private long computeWorkerExpiryThresholdInSecond() {
            final long timeInSeconds = Duration.ofMillis(System.currentTimeMillis()
                            - DEFAULT_NO_OF_SKIP_STAT_FOR_DEAD_WORKER_THRESHOLD
                                    * config.workerMetricsReporterFreqInMillis())
                    .getSeconds();
            log.info("WorkerMetricStats expiry time in seconds : {}", timeInSeconds);
            return timeInSeconds;
        }

        /**
         * Looks at inMemoryWorkerToLeasesMapping for lease assignment and figures out if there is room considering
         * any new assignment that would have happened.
         */
        public boolean isWorkerTotalThroughputLessThanMaxThroughput(final String workerId) {
            return getTotalAssignedThroughput(workerId) <= config.maxThroughputPerHostKBps();
        }

        /**
         * Looks at inMemoryWorkerToLeasesMapping for lease assignment of a worker and returns true if the worker has
         * no leases assigned or less than maxNumberOfLeasesPerHost else false.
         */
        public boolean isWorkerAssignedLeasesLessThanMaxLeases(final String workerId) {
            final Set<Lease> assignedLeases = workerToLeasesMap.get(workerId);
            if (CollectionUtils.isEmpty(assignedLeases)) {
                // There are no leases assigned to the worker, that means its less than maxNumberOfLeasesPerHost.
                return true;
            } else {
                return assignedLeases.size() < maxLeasesForWorker;
            }
        }

        public Double getTotalAssignedThroughput(final String workerId) {
            return workerToTotalAssignedThroughputMap.getOrDefault(workerId, 0D);
        }

        private CompletableFuture<List<WorkerMetricStats>> loadWorkerMetricStats() {
            return CompletableFuture.supplyAsync(() -> loadWithRetry(workerMetricsDAO::getAllWorkerMetricStats));
        }

        private CompletableFuture<Map.Entry<List<Lease>, List<String>>> loadLeaseListAsync() {
            return CompletableFuture.supplyAsync(() -> loadWithRetry(() -> leaseRefresher.listLeasesParallely(
                    LEASE_ASSIGNMENT_CALL_THREAD_POOL, DEFAULT_LEASE_TABLE_SCAN_PARALLELISM_FACTOR)));
        }

        private <T> T loadWithRetry(final Callable<T> loadFunction) {
            int retryAttempt = 0;
            while (true) {
                try {
                    return loadFunction.call();
                } catch (final Exception e) {
                    if (retryAttempt < DDB_LOAD_RETRY_ATTEMPT) {
                        log.warn(
                                "Failed to load : {}, retrying",
                                loadFunction.getClass().getName(),
                                e);
                        retryAttempt++;
                    } else {
                        throw new CompletionException(e);
                    }
                }
            }
        }
    }

    private long getCheckpointOwnerTimeoutTimestampMillis() {
        // this is a future timestamp in millis that the graceful lease handoff shutdown can be considered
        // expired. LeaseDurationMillis is used here to account for how long it might take for the
        // lease owner to receive the shutdown signal before executing shutdown.
        return getNanoTimeMillis()
                + gracefulLeaseHandoffConfig.gracefulLeaseHandoffTimeoutMillis()
                + leaseDurationMillis;
    }

    private long getNanoTimeMillis() {
        // this is not a wall clock time. But if we stick with using this time provider for calculating the elapsed
        // time it should be okay to use in checkpoint expiration calculation.
        return TimeUnit.NANOSECONDS.toMillis(nanoTimeProvider.get());
    }

    private static boolean isSameOwners(Lease currentLease, Lease previousLease) {
        return Objects.equals(currentLease.leaseOwner(), previousLease.leaseOwner())
                && Objects.equals(currentLease.checkpointOwner(), previousLease.checkpointOwner());
    }
}
