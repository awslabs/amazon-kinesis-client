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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.coordinator.streamInfo.StreamIdCacheManager;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfo;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseAssignmentStrategy;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.metrics.NullMetricsScope;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.SHARD_END;

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

    private static final String FORCE_LEADER_RELEASE_METRIC_NAME = "ForceLeaderRelease";

    /**
     * Internal threadpool used to parallely perform assignment operation by calling storage.
     */
    private static final ExecutorService LEASE_ASSIGNMENT_CALL_THREAD_POOL =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private static final String METRICS_LEASE_ASSIGNMENT_MANAGER = "LeaseAssignmentManager";
    /** Upper bound on SHARD_END lease deletions per LAM run, so cleanup can't stretch a run past its interval. */
    @VisibleForTesting
    static final int MAX_SHARD_END_LEASE_DELETIONS_PER_RUN = 100;

    private static final String METRICS_INCOMPLETE_EXPIRED_LEASES_ASSIGNMENT =
            "LeaseAssignmentManager.IncompleteExpiredLeasesAssignment";

    private final LeaseRefresher leaseRefresher;
    private final LeaderDecider leaderDecider;
    private final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config;
    private final String currentWorkerId;
    private final Long leaseDurationMillis;
    private final MetricsFactory metricsFactory;
    private final ScheduledExecutorService executorService;
    private final Supplier<Long> nanoTimeProvider;
    private final int maxLeasesForWorker;
    private final LeaseManagementConfig.GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig;
    private final LeaseAssignmentStrategy leaseAssignmentStrategy;
    private boolean tookOverLeadershipInThisRun = false;
    private final long leaseAssignmentIntervalMillis;
    private final StreamIdCacheManager streamIdCacheManager;
    private final LAMDataManager lamDataManager;
    /** This worker's LeaseCleanupManager, which decides whether a SHARD_END lease can be deleted and deletes it. */
    private final LeaseCleanupManager leaseCleanupManager;

    /**
     * Lease snapshot class to reduce memory usage by only containing attributes
     * needed for subsequent checks rather than the entire lease.
     */
    @RequiredArgsConstructor
    private static class LeaseCounterSnapshot {
        final long leaseCounter;
        final long lastCounterIncrementNanos;
        final boolean shutdownRequested;
        final Long checkpointOwnerTimeoutTimestampMillis;
        final String leaseOwner;
        final String checkpointOwner;
    }

    private final Map<String, LeaseCounterSnapshot> prevRunLeasesState = new HashMap<>();

    private Future<?> managerFuture;

    private int noOfContinuousFailedAttempts = 0;
    private int lamRunCounter = 0;

    public synchronized void start() {
        if (isNull(managerFuture)) {
            // LAM can be dynamically started/stopped and restarted during MigrationStateMachine execution
            // so reset the flag to refresh the state before processing during a restart of LAM.
            tookOverLeadershipInThisRun = false;
            managerFuture = executorService.scheduleWithFixedDelay(
                    this::performAssignment, 0L, (int) (leaseAssignmentIntervalMillis), TimeUnit.MILLISECONDS);
            log.info("Started LeaseAssignmentManager using {}", leaseAssignmentStrategy);
            return;
        }
        log.info("LeaseAssignmentManager already running...");
    }

    public synchronized void stop() {
        if (nonNull(managerFuture)) {
            managerFuture.cancel(true);
            managerFuture = null;
            log.info("Completed shutdown of LeaseAssignmentManager");
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

            final LeaseAssignmentDecider leaseAssignmentDecider = getLeaseAssignmentDecider(inMemoryStorageView);

            // lastCounterIncrementNanos was already set while loading the view (it's a sort key for the shard
            // tree order); see loadInMemoryStorageView.

            // This does not include the leases from the worker that has expired (based on WorkerMetricStats's
            // lastUpdateTime)
            // but the lease is not expired (based on the leaseCounter on lease).
            // If a worker has died, the lease will be expired and assigned in next iteration.
            // The lease list is in shard tree order (see loadInMemoryStorageView), and the deciders assign in the
            // order given, so after a reshard the leases that unblock the rest of the tree are assigned first.
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
            success = true;
            noOfContinuousFailedAttempts = 0;
        } catch (final Throwable e) {
            // If the thread was interrupted (e.g., via stop()/cancel(true)), restore the interrupt flag
            // so the ScheduledExecutorService can detect the cancellation and stop re-scheduling.
            if (Thread.currentThread().isInterrupted()
                    || e instanceof InterruptedException
                    || (e instanceof CompletionException && e.getCause() instanceof InterruptedException)) {
                log.info("LeaseAssignmentManager interrupted, stopping assignment.", e);
                Thread.currentThread().interrupt();
            } else {
                log.error("LeaseAssignmentManager failed to perform lease assignment.", e);
                noOfContinuousFailedAttempts++;

                final boolean isError = e instanceof Error;
                final int failureThreshold = isError ? 1 : DEFAULT_FAILURE_COUNT_TO_SWITCH_LEADER;

                if (noOfContinuousFailedAttempts >= failureThreshold) {
                    log.error(
                            "Failed to perform assignment {} times in a row{}, releasing leadership from worker : {}",
                            failureThreshold,
                            isError ? " (Error detected)" : "",
                            currentWorkerId);
                    MetricsUtil.addCount(metricsScope, FORCE_LEADER_RELEASE_METRIC_NAME, 1, MetricsLevel.SUMMARY);
                    leaderDecider.releaseLeadershipIfHeld();
                }
            }
        } finally {
            MetricsUtil.addSuccessAndLatency(metricsScope, success, startTime, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(metricsScope);
        }
    }

    private LeaseAssignmentDecider getLeaseAssignmentDecider(InMemoryStorageView inMemoryStorageView) {
        final LeaseAssignmentDecider leaseAssignmentDecider;
        if (leaseAssignmentStrategy == LeaseAssignmentStrategy.LEASE_COUNT_BASED) {
            leaseAssignmentDecider = new LeaseCountBasedLeaseAssignmentDecider(
                    inMemoryStorageView, maxLeasesForWorker, nanoTimeProvider);
        } else {
            leaseAssignmentDecider = new VarianceBasedLeaseAssignmentDecider(
                    inMemoryStorageView,
                    config.dampeningPercentage(),
                    config.reBalanceThresholdPercentage(),
                    config.allowThroughputOvershoot(),
                    config.useAbsoluteReBalanceThreshold());
        }
        return leaseAssignmentDecider;
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
            resolveStreamId(lease);
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
            resolveStreamId(lease);
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

    /**
     * Updates all new leases with currentTime if the counter is updated since last run else keeps whatever was prev,
     * and updates the prevRunLeasesState.
     *
     * <p>Called from {@code InMemoryStorageView.loadInMemoryStorageView} on every lease in the snapshot, before
     * the leases are sorted into shard tree order (which sorts on lastCounterIncrementNanos last). It used to be
     * called from {@code performAssignment} right after loading; see loadInMemoryStorageView for why it moved and
     * why the values are the same. One difference: if a run fails after this call but before assignment, the next
     * run compares against this run's state rather than the run before it. A counter that increased between the two
     * scans then gets this run's scan time instead of the next run's, which is when the increase was first seen.
     */
    private void updateLeasesLastCounterIncrementNanosAndLeaseShutdownTimeout(
            final List<Lease> leaseList, final Long scanTime) {
        for (final Lease lease : leaseList) {
            final LeaseCounterSnapshot prev = prevRunLeasesState.get(lease.leaseKey());

            // make sure lease shutdown timeouts are tracked.
            if (lease.shutdownRequested()) {
                // previous and current leases might have same next and checkpoint owners but there is no
                // guarantee that the latest shutdown is the same shutdown in the previous lease for example
                // some other leaders change the lease states while this worker waiting for it's LAM run.
                // This is the best effort to prevent marking the incorrect timeout.
                if (isNull(prev) || !prev.shutdownRequested || !isSameOwners(lease, prev)) {
                    // Add new value if previous is null, previous lease is not shutdown pending or the owners
                    // don't match
                    lease.checkpointOwnerTimeoutTimestampMillis(getCheckpointOwnerTimeoutTimestampMillis());
                } else {
                    lease.checkpointOwnerTimeoutTimestampMillis(prev.checkpointOwnerTimeoutTimestampMillis);
                }
            }

            if (isNull(prev)) {
                lease.lastCounterIncrementNanos(
                        isNull(lease.actualOwner())
                                // This is an unassigned lease, mark as 0L that puts this in first in assignment order
                                ? 0L
                                : scanTime);
            } else {
                lease.lastCounterIncrementNanos(
                        lease.leaseCounter() > prev.leaseCounter ? scanTime : prev.lastCounterIncrementNanos);
            }
        }
        prevRunLeasesState.clear();
        for (Lease lease : leaseList) {
            prevRunLeasesState.put(
                    lease.leaseKey(),
                    new LeaseCounterSnapshot(
                            lease.leaseCounter(),
                            lease.lastCounterIncrementNanos(),
                            lease.shutdownRequested(),
                            lease.checkpointOwnerTimeoutTimestampMillis(),
                            lease.leaseOwner(),
                            lease.checkpointOwner()));
        }
    }

    /**
     * Deletes SHARD_END leases whose children have all checkpointed and whose parents are gone, and evicts the rest
     * so their owners stop renewing them. Either way, they're left out of the returned list, so they're never
     * assigned or rebalanced and never count toward maxLeasesForWorker. Runs synchronously before assignment, so the
     * per-worker lease counts the deciders see only include leases that still need processing. Returns the leases
     * left for assignment.
     *
     * <p>A SHARD_END lease is left out even if its delete or eviction fails (the write throws, or the eviction's
     * conditional write doesn't apply), and even though its owner then still holds and renews it. That's deliberate,
     * for two reasons:
     * <ul>
     *   <li>maxLeasesForWorker limits how many shards a worker processes, and a SHARD_END lease's shard is done: its
     *       owner's consumer has already finished it, so all the lease costs the owner is renewal writes. Counting it
     *       would hold a processing slot for a shard that needs no processing.</li>
     *   <li>Counting it would bring back the stall this method exists to prevent. Writes are most likely to fail
     *       under lease table throttling during a large reshard, which is exactly when the children need those slots.
     *       Counting finished leases toward the limit is what kept child leases unassigned before this change.</li>
     * </ul>
     * The write is retried on the next run, since the lease is still at SHARD_END in the lease table. Leaving the
     * lease in the list instead wouldn't only count it: the list is also what the deciders assign from (an expired
     * lease would be reassigned) and what rebalancing moves leases from (a finished lease could be handed off to
     * another worker), so counting it correctly would need a separate count-only set that both deciders know to skip.
     *
     * <p>Deletion uses the workers' LeaseCleanupManager rule
     * ({@link LeaseCleanupManager#deleteCompletedShardLeaseIfReady}, which also explains why the rule exists), but
     * checks it against this run's lease table snapshot instead of re-reading the lease, its children, and its
     * parents from DynamoDB. A stale snapshot is safe: both conditions only ever move one way (a child never returns
     * to its initial position, a deleted parent stays deleted), so older data can only say "not ready yet". When
     * cleanupLeasesUponShardCompletion is false, nothing is deleted and SHARD_END leases are only evicted.
     *
     * <p>SHARD_END leases without childShardIds are skipped entirely: they aren't deleted or evicted here, and they
     * stay in the returned list, so they can be assigned and they count toward maxLeasesForWorker like any other
     * lease. They only come from a fetch that hit ResourceNotFound (for example, the stream was deleted), which ends
     * the shard with no child shards; a split or merge always records the child shards first. The deletion rule
     * needs childShardIds, so this snapshot can't say when such a lease is safe to delete. The way to decide is to
     * ask Kinesis for the shard's children: if Kinesis returns ResourceNotFound, the shard no longer exists and the
     * lease can be deleted. The workers' LeaseCleanupManager does that, so these leases rely on a worker's
     * LeaseCleanupManager queue to be deleted, in one of two ways:
     * <ol>
     *   <li>The worker that processed the shard end queued the lease when its ShutdownTask finished. That queue is
     *       in memory, and the entry stays even if the worker later loses the lease (it expires, or a rebalance moves
     *       it), so that worker still deletes the lease, whoever owns it by then.</li>
     *   <li>If the lease is assigned again (for example, because the worker that queued it restarted and lost its
     *       queue, and the lease expired), the new owner's ShardConsumer goes straight to a SHARD_END shutdown, and
     *       its ShutdownTask queues the lease in that worker's LeaseCleanupManager.</li>
     * </ol>
     * Until one of them deletes the lease, it holds a slot on whichever worker owns it. A possible future change is
     * for the leader to handle these leases too (for example, by asking Kinesis for the shard's children itself, or
     * by evicting them like other SHARD_END leases), so they stop depending on a worker's LeaseCleanupManager.
     */
    private List<Lease> handleShardEndLeases(
            final List<Lease> leasesInShardTreeOrder, final MetricsScope metricsScope) {
        // The deletion check looks up parents and children by lease key, so index the leases by key first.
        // toMap(key, value, merge): key = leaseKey, value = the lease, and (a, b) -> a keeps the first lease if a
        // key repeats instead of throwing. The pass below removes each lease it deletes from this map, so by the time
        // it reaches a child, the child's deleted parents are already missing from the map.
        final Map<String, Lease> leaseByLeaseKey =
                leasesInShardTreeOrder.stream().collect(Collectors.toMap(Lease::leaseKey, lease -> lease, (a, b) -> a));
        final Set<String> shardEndLeaseKeys = new HashSet<>();
        int deleted = 0;
        int evicted = 0;

        // Delete top-down, in one pass: a lease can only be deleted once its parents are gone (each parent's
        // deletion check needs its child leases to still exist), and shard tree order visits every parent before
        // its children, so a parent deleted earlier in this pass lets its children be deleted too. Leases that
        // aren't ready are evicted instead, so they stop holding a slot on their worker and stop renewing. (Past the
        // sorter's depth limit a child can come before its parent; the check then just says not ready, and the
        // child is deleted on a later run.)
        for (Lease lease : leasesInShardTreeOrder) {
            if (!SHARD_END.equals(lease.checkpoint()) || lease.childShardIds().isEmpty()) {
                // Not at SHARD_END: a normal lease. At SHARD_END without childShardIds: left to a worker's
                // LeaseCleanupManager (see this method's Javadoc). Either way, it stays in the list for assignment.
                continue;
            }
            // left out of the returned list whether or not the delete or eviction below succeeds (see the Javadoc)
            shardEndLeaseKeys.add(lease.leaseKey());
            if (deleted < MAX_SHARD_END_LEASE_DELETIONS_PER_RUN) {
                try {
                    if (leaseCleanupManager.deleteCompletedShardLeaseIfReady(
                            lease, shardInfoFor(lease), lease.childShardIds(), leaseByLeaseKey::get)) {
                        leaseByLeaseKey.remove(lease.leaseKey());
                        deleted++;
                        continue; // deleted, so nothing to evict
                    }
                } catch (IllegalStateException e) {
                    // a child lease is missing from the snapshot; not ready
                    log.warn("Not deleting SHARD_END lease {}: {}", lease.leaseKey(), e.getMessage());
                } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
                    log.warn("Failed to delete SHARD_END lease {}; will retry next run", lease.leaseKey(), e);
                }
            }
            if (nonNull(lease.leaseOwner())) {
                try {
                    if (leaseRefresher.evictLease(lease)) {
                        evicted++;
                    }
                } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
                    log.warn("Failed to evict SHARD_END lease {}; will retry next run", lease.leaseKey(), e);
                }
            }
        }

        log.info(
                "SHARD_END leases: {} handled by the leader, {} deleted, {} evicted",
                shardEndLeaseKeys.size(),
                deleted,
                evicted);
        metricsScope.addData("ShardEndLeasesDeleted", deleted, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        metricsScope.addData("ShardEndLeasesEvicted", evicted, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        return leasesInShardTreeOrder.stream()
                .filter(lease -> !shardEndLeaseKeys.contains(lease.leaseKey()))
                .collect(Collectors.toList());
    }

    /**
     * ShardInfo for deriving parent and child lease keys (which add the stream prefix in multi-stream mode); only the
     * shard ID and stream are needed. Not built with {@code DynamoDBLeaseCoordinator.convertLeaseToAssignment}, which
     * is for a lease the worker holds: it copies the lease's concurrency token, a random ID the owning worker gives
     * the lease in memory when it acquires it, and checks on every checkpoint to reject writes from a ShardConsumer
     * left over from an earlier time it held the lease. The token isn't stored in the lease table, so leases from the
     * scan don't have one.
     */
    private static ShardInfo shardInfoFor(final Lease lease) {
        if (lease instanceof MultiStreamLease) {
            final MultiStreamLease multiStreamLease = (MultiStreamLease) lease;
            return new ShardInfo(
                    multiStreamLease.shardId(),
                    null,
                    lease.parentShardIds(),
                    lease.checkpoint(),
                    multiStreamLease.streamIdentifier());
        }
        return new ShardInfo(lease.leaseKey(), null, lease.parentShardIds(), lease.checkpoint());
    }

    private void prepareAfterLeaderSwitch() {
        prevRunLeasesState.clear();
        noOfContinuousFailedAttempts = 0;
    }

    // Resolves and caches the stream ID for a given lease during lease acquisition.
    // If resolution fails, it will be retried when the stream ID is actually needed for processing.
    private void resolveStreamId(Lease lease) {
        try {
            StreamIdentifier streamIdentifier = null;
            if (lease instanceof MultiStreamLease) {
                streamIdentifier = StreamIdentifier.multiStreamInstance(
                        StreamInfo.multiStreamLeaseKeyToStreamIdentifier(lease.leaseKey()));
            }
            streamIdCacheManager.resolveStreamId(streamIdentifier);
        } catch (Exception e) {
            // we don't want leaseTaker to be blocked, streamId resolution can be retried when streamId is needed
            log.warn("Failed to resolve stream id for lease key {}", lease.leaseKey(), e);
        }
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
         * List of active worker metrics eligible for lease assignment. The {@link LAMDataManager}
         * provides valid, non-expired metrics; LAM further excludes workers with failing metrics.
         */
        private List<WorkerMetricStats> activeWorkerMetrics;
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
         * Loads leases and worker metrics via {@link LAMDataManager} and populates data structures
         * used in lease assignment.
         *
         * <p>The {@link LAMDataSnapshot} returned by the manager contains valid, non-expired worker
         * metrics. This method further filters out workers with failing metrics before using them
         * for assignment decisions.</p>
         *
         * <p>Leases are prepared in this order, and each step depends on the one before it:
         * <ol>
         *   <li>Record {@code leaseTableScanTime} right after the scan, as before.</li>
         *   <li>Set each lease's lastCounterIncrementNanos (and graceful shutdown timeout) via
         *       {@code updateLeasesLastCounterIncrementNanosAndLeaseShutdownTimeout}. This used to run in
         *       {@code performAssignment} after this method; it runs here now because the next step sorts on
         *       lastCounterIncrementNanos. Its input is unchanged: every lease in the snapshot (SHARD_END leases
         *       included) and the same scan time. Nothing that used to run between its old and new position reads
         *       the fields it sets, so the values, and when they're used, are the same as before.</li>
         *   <li>Sort every lease into shard tree order ({@link ShardTreeSorter}): parents before children, ready
         *       leases (every parent lease at SHARD_END or gone) before ones still waiting on a parent, and
         *       lastCounterIncrementNanos last. With no shard tree relationships among the leases (every lease at
         *       depth 0, as when no reshard is in progress), this is exactly the order the lease assignment deciders
         *       used to sort into themselves: unassigned leases (0) first, then the least recently renewed.</li>
         *   <li>Delete or evict SHARD_END leases ({@code handleShardEndLeases}). Walking them in shard tree order
         *       deletes parents before children in a single pass, which the deletion check requires.</li>
         *   <li>Build the view from the leases left, which keep the shard tree order. That's the order they're
         *       offered for assignment: {@code performAssignment} filters this list without reordering it, and
         *       the deciders assign in the order given rather than sorting again. The worker lease counts are
         *       built after the SHARD_END leases are removed, so those leases never count toward
         *       maxLeasesForWorker, even when the leader's delete or eviction of them failed.</li>
         * </ol>
         */
        public void loadInMemoryStorageView(final MetricsScope metricsScope) {
            final LAMDataSnapshot snapshot = lamDataManager.loadData(metricsScope);
            this.leaseTableScanTime = nanoTimeProvider.get();

            // Set each lease's lastCounterIncrementNanos (and graceful shutdown timeout) first, for every lease in
            // the snapshot: the shard tree order uses it as its last sort key.
            updateLeasesLastCounterIncrementNanosAndLeaseShutdownTimeout(snapshot.getLeases(), leaseTableScanTime);

            // Every lease, SHARD_END leases included, in shard tree order: parents before children, ready leases
            // before ones still waiting on a parent, then unassigned before expired (lastCounterIncrementNanos).
            final List<Lease> leasesInShardTreeOrder = ShardTreeSorter.sort(snapshot.getLeases());

            // SHARD_END leases are deleted or evicted by the leader and left out of the view, whether or not those
            // writes succeed, so they're never assigned and never counted toward maxLeasesForWorker by either
            // decider (see handleShardEndLeases for why). The leases left keep the shard tree order, which is the
            // order they're offered for assignment.
            this.leaseList = handleShardEndLeases(leasesInShardTreeOrder, metricsScope);
            log.info("Total Leases available : {}", leaseList.size());

            // The snapshot contains valid, non-expired worker metrics from the manager.
            // Filter out workers with failing metrics — those should not receive new assignments.
            final List<WorkerMetricStats> workerMetricsFromSnapshot = snapshot.getWorkerMetricStats();
            log.info("Total valid WorkerMetricStats from manager: {}", workerMetricsFromSnapshot.size());

            final long countOfWorkersWithFailingMetric = workerMetricsFromSnapshot.stream()
                    .filter(WorkerMetricStats::isAnyWorkerMetricFailing)
                    .count();
            if (countOfWorkersWithFailingMetric > 0) {
                metricsScope.addData(
                        "NumWorkersWithFailingWorkerMetric",
                        countOfWorkersWithFailingMetric,
                        StandardUnit.COUNT,
                        MetricsLevel.SUMMARY);
            }

            this.activeWorkerMetrics = workerMetricsFromSnapshot.stream()
                    .filter(wm -> !wm.isAnyWorkerMetricFailing())
                    .collect(Collectors.toList());
            log.info("Total active WorkerMetricStats (excluding failing): {}", activeWorkerMetrics.size());

            final double averageLeaseThroughput = leaseList.stream()
                    .filter(lease -> nonNull(lease.throughputKBps()))
                    .mapToDouble(Lease::throughputKBps)
                    .average()
                    // If none of the leases has any value, that means its app
                    // startup time and thus assigns 0 in that case to start with.
                    .orElse(0D);

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

    private static boolean isSameOwners(Lease currentLease, LeaseCounterSnapshot previousLease) {
        return Objects.equals(currentLease.leaseOwner(), previousLease.leaseOwner)
                && Objects.equals(currentLease.checkpointOwner(), previousLease.checkpointOwner);
    }
}
