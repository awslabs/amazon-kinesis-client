/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.leases.dynamodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseTaker;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static software.amazon.kinesis.common.CommonCalculations.getRenewerTakerIntervalMillis;

/**
 * An implementation of {@link LeaseTaker} that uses DynamoDB via {@link LeaseRefresher}.
 */
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLeaseTaker implements LeaseTaker {
    private static final int TAKE_RETRIES = 3;
    private static final int SCAN_RETRIES = 1;
    private static final double RENEWAL_SLACK_PERCENTAGE = 0.5;

    // See note on TAKE_LEASES_DIMENSION(Callable) for why we have this callable.
    private static final Callable<Long> SYSTEM_CLOCK_CALLABLE = System::nanoTime;

    private static final String TAKE_LEASES_DIMENSION = "TakeLeases";

    private final LeaseRefresher leaseRefresher;
    private final String workerIdentifier;
    private final long leaseDurationNanos;
    private final long leaseRenewalIntervalMillis;
    private final MetricsFactory metricsFactory;

    final Map<String, Lease> allLeases = new HashMap<>();
    // TODO: Remove these defaults and use the defaults in the config
    private int maxLeasesForWorker = Integer.MAX_VALUE;
    private int maxLeasesToStealAtOneTime = 1;
    private boolean enablePriorityLeaseAssignment = true;
    private int veryOldLeaseDurationNanosMultiplier = 3;
    private long lastScanTimeNanos = 0L;

    public DynamoDBLeaseTaker(
            LeaseRefresher leaseRefresher,
            String workerIdentifier,
            long leaseDurationMillis,
            final MetricsFactory metricsFactory) {
        this.leaseRefresher = leaseRefresher;
        this.workerIdentifier = workerIdentifier;
        this.leaseRenewalIntervalMillis = getRenewerTakerIntervalMillis(leaseDurationMillis, 0);
        this.leaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
        this.metricsFactory = metricsFactory;
    }

    /**
     * Worker will not acquire more than the specified max number of leases even if there are more
     * shards that need to be processed. This can be used in scenarios where a worker is resource constrained or
     * to prevent lease thrashing when small number of workers pick up all leases for small amount of time during
     * deployment.
     * Note that setting a low value may cause data loss (e.g. if there aren't enough Workers to make progress on all
     * shards). When setting the value for this property, one must ensure enough workers are present to process
     * shards and should consider future resharding, child shards that may be blocked on parent shards, some workers
     * becoming unhealthy, etc.
     *
     * @param maxLeasesForWorker Max leases this Worker can handle at a time
     * @return LeaseTaker
     */
    public DynamoDBLeaseTaker withMaxLeasesForWorker(int maxLeasesForWorker) {
        if (maxLeasesForWorker <= 0) {
            throw new IllegalArgumentException("maxLeasesForWorker should be >= 1");
        }
        this.maxLeasesForWorker = maxLeasesForWorker;
        return this;
    }

    /**
     * Overrides the default very old lease duration nanos multiplier to increase the threshold for taking very old leases.
     * Setting this to a higher value than 3 will increase the threshold for very old lease taking.
     *
     * @param veryOldLeaseDurationNanosMultiplier Very old lease duration multiplier for adjusting very old lease taking.
     * @return LeaseTaker
     */
    public DynamoDBLeaseTaker withVeryOldLeaseDurationNanosMultiplier(int veryOldLeaseDurationNanosMultiplier) {
        this.veryOldLeaseDurationNanosMultiplier = veryOldLeaseDurationNanosMultiplier;
        return this;
    }

    public DynamoDBLeaseTaker withEnablePriorityLeaseAssignment(boolean enablePriorityLeaseAssignment) {
        this.enablePriorityLeaseAssignment = enablePriorityLeaseAssignment;
        return this;
    }

    /**
     * Max leases to steal from a more loaded Worker at one time (for load balancing).
     * Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
     * but can cause higher churn in the system.
     *
     * @param maxLeasesToStealAtOneTime Steal up to this many leases at one time (for load balancing)
     * @return LeaseTaker
     */
    public DynamoDBLeaseTaker withMaxLeasesToStealAtOneTime(int maxLeasesToStealAtOneTime) {
        if (maxLeasesToStealAtOneTime <= 0) {
            throw new IllegalArgumentException("maxLeasesToStealAtOneTime should be >= 1");
        }
        this.maxLeasesToStealAtOneTime = maxLeasesToStealAtOneTime;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Lease> takeLeases() throws DependencyException, InvalidStateException {
        return takeLeases(SYSTEM_CLOCK_CALLABLE);
    }

    /**
     * Internal implementation of TAKE_LEASES_DIMENSION. Takes a callable that can provide the time to enable test cases
     * without Thread.sleep. Takes a callable instead of a raw time value because the time needs to be computed as-of
     * immediately after the scan.
     *
     * @param timeProvider
     *            Callable that will supply the time
     *
     * @return map of lease key to taken lease
     *
     * @throws DependencyException
     * @throws InvalidStateException
     */
    synchronized Map<String, Lease> takeLeases(Callable<Long> timeProvider)
            throws DependencyException, InvalidStateException {
        // Key is leaseKey
        Map<String, Lease> takenLeases = new HashMap<>();

        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, TAKE_LEASES_DIMENSION);

        long startTime = System.currentTimeMillis();
        long updateAllLeasesTotalTimeMillis;
        boolean success = false;

        ProvisionedThroughputException lastException = null;

        try {
            try {
                for (int i = 1; i <= SCAN_RETRIES; i++) {
                    try {
                        updateAllLeases(timeProvider);
                        success = true;
                    } catch (ProvisionedThroughputException e) {
                        log.info(
                                "Worker {} could not find available leases on try {} out of {}",
                                workerIdentifier,
                                i,
                                TAKE_RETRIES);
                        lastException = e;
                    }
                }
            } finally {
                updateAllLeasesTotalTimeMillis = System.currentTimeMillis() - startTime;
                MetricsUtil.addWorkerIdentifier(scope, workerIdentifier);
                MetricsUtil.addSuccessAndLatency(scope, "ListLeases", success, startTime, MetricsLevel.DETAILED);
            }

            if (lastException != null) {
                log.error(
                        "Worker {} could not scan leases table, aborting TAKE_LEASES_DIMENSION. Exception caught by"
                                + " last retry:",
                        workerIdentifier,
                        lastException);
                return takenLeases;
            }

            List<Lease> availableLeases = getAvailableLeases();

            Set<Lease> leasesToTake = computeLeasesToTake(availableLeases, timeProvider);
            leasesToTake = updateStaleLeasesWithLatestState(updateAllLeasesTotalTimeMillis, leasesToTake);

            Set<String> untakenLeaseKeys = new HashSet<>();

            for (Lease lease : leasesToTake) {
                String leaseKey = lease.leaseKey();

                startTime = System.currentTimeMillis();
                success = false;
                try {
                    for (int i = 1; i <= TAKE_RETRIES; i++) {
                        try {
                            if (leaseRefresher.takeLease(lease, workerIdentifier)) {
                                lease.lastCounterIncrementNanos(System.nanoTime());
                                takenLeases.put(leaseKey, lease);
                            } else {
                                untakenLeaseKeys.add(leaseKey);
                            }

                            success = true;
                            break;
                        } catch (ProvisionedThroughputException e) {
                            log.info(
                                    "Could not take lease with key {} for worker {} on try {} out of {} due to"
                                            + " capacity",
                                    leaseKey,
                                    workerIdentifier,
                                    i,
                                    TAKE_RETRIES);
                        }
                    }
                } finally {
                    MetricsUtil.addSuccessAndLatency(scope, "TakeLease", success, startTime, MetricsLevel.DETAILED);
                }
            }

            if (takenLeases.size() > 0) {
                log.info(
                        "Worker {} successfully took {} leases: {}",
                        workerIdentifier,
                        takenLeases.size(),
                        stringJoin(takenLeases.keySet(), ", "));
            }

            if (untakenLeaseKeys.size() > 0) {
                log.info(
                        "Worker {} failed to take {} leases: {}",
                        workerIdentifier,
                        untakenLeaseKeys.size(),
                        stringJoin(untakenLeaseKeys, ", "));
            }

            scope.addData("TakenLeases", takenLeases.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData("UntakenLeases", untakenLeaseKeys.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
        } finally {
            MetricsUtil.endScope(scope);
        }

        return takenLeases;
    }

    /**
     * If update all leases takes longer than the lease renewal time,
     * we fetch the latest lease info for the given leases that are marked for lease steal.
     * If nothing is found (or any transient network error occurs),
     * we default to the last known state of the lease
     *
     * @param updateAllLeasesEndTime How long it takes for update all leases to complete
     * @return set of leases to take.
     */
    private Set<Lease> updateStaleLeasesWithLatestState(long updateAllLeasesEndTime, Set<Lease> leasesToTake) {
        if (updateAllLeasesEndTime > leaseRenewalIntervalMillis * RENEWAL_SLACK_PERCENTAGE) {
            leasesToTake = leasesToTake.stream()
                    .map(lease -> {
                        if (lease.isMarkedForLeaseSteal()) {
                            try {
                                log.debug("Updating stale lease {}.", lease.leaseKey());
                                return leaseRefresher.getLease(lease.leaseKey());
                            } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
                                log.warn(
                                        "Failed to fetch latest state of the lease {} that needs to be stolen, "
                                                + "defaulting to existing lease",
                                        lease.leaseKey(),
                                        e);
                            }
                        }
                        return lease;
                    })
                    .collect(Collectors.toSet());
        }
        return leasesToTake;
    }

    /** Package access for testing purposes.
     *
     * @param strings
     * @param delimiter
     * @return Joined string.
     */
    static String stringJoin(Collection<String> strings, String delimiter) {
        StringBuilder builder = new StringBuilder();
        boolean needDelimiter = false;
        for (String string : strings) {
            if (needDelimiter) {
                builder.append(delimiter);
            }
            builder.append(string);
            needDelimiter = true;
        }

        return builder.toString();
    }

    /**
     * Scan all leases and update lastRenewalTime. Add new leases and delete old leases.
     *
     * @param timeProvider callable that supplies the current time
     *
     * @return list of available leases, possibly empty, never null.
     *
     * @throws ProvisionedThroughputException if listLeases fails due to lack of provisioned throughput
     * @throws InvalidStateException if the lease table does not exist
     * @throws DependencyException if listLeases fails in an unexpected way
     */
    private void updateAllLeases(Callable<Long> timeProvider)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Lease> freshList = leaseRefresher.listLeases();
        try {
            lastScanTimeNanos = timeProvider.call();
        } catch (Exception e) {
            throw new DependencyException("Exception caught from timeProvider", e);
        }

        // This set will hold the lease keys not updated by the previous listLeases call.
        Set<String> notUpdated = new HashSet<>(allLeases.keySet());

        // Iterate over all leases, finding ones to try to acquire that haven't changed since the last iteration
        for (Lease lease : freshList) {
            String leaseKey = lease.leaseKey();

            final Lease oldLease = allLeases.put(leaseKey, lease);
            notUpdated.remove(leaseKey);

            if (oldLease != null) {
                // If we've seen this lease before...
                if (oldLease.leaseCounter().equals(lease.leaseCounter())) {
                    // ...and the counter hasn't changed, propagate the lastRenewalNanos time from the old lease
                    lease.lastCounterIncrementNanos(oldLease.lastCounterIncrementNanos());
                } else {
                    // ...and the counter has changed, set lastRenewalNanos to the time of the scan.
                    lease.lastCounterIncrementNanos(lastScanTimeNanos);
                }
            } else {
                if (lease.leaseOwner() == null) {
                    // if this new lease is unowned, it's never been renewed.
                    lease.lastCounterIncrementNanos(0L);

                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Treating new lease with key {} as never renewed because it is new and unowned.",
                                leaseKey);
                    }
                } else {
                    // if this new lease is owned, treat it as renewed as of the scan
                    lease.lastCounterIncrementNanos(lastScanTimeNanos);
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Treating new lease with key {} as recently renewed because it is new and owned.",
                                leaseKey);
                    }
                }
            }
        }

        // Remove dead leases from allLeases
        for (String key : notUpdated) {
            allLeases.remove(key);
        }
    }

    /**
     * @return list of leases that available as of our last scan.
     */
    private List<Lease> getAvailableLeases() {
        return allLeases.values().stream()
                .filter(lease -> lease.isAvailable(leaseDurationNanos, lastScanTimeNanos))
                .collect(Collectors.toList());
    }

    /**
     * Compute the number of leases I should try to take based on the state of the system.
     *
     * @param availableLeases list of leases we determined to be available
     * @param timeProvider callable which returns the current time in nanos
     * @return set of leases to take.
     */
    @VisibleForTesting
    Set<Lease> computeLeasesToTake(List<Lease> availableLeases, Callable<Long> timeProvider)
            throws DependencyException {
        Map<String, Integer> leaseCounts = computeLeaseCounts(availableLeases);
        Set<Lease> leasesToTake = new HashSet<>();
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, TAKE_LEASES_DIMENSION);
        MetricsUtil.addWorkerIdentifier(scope, workerIdentifier);

        final int numAvailableLeases = availableLeases.size();
        final int numLeases = allLeases.size();
        final int numWorkers = leaseCounts.size();
        int numLeasesToReachTarget = 0;
        int leaseSpillover = 0;
        int veryOldLeaseCount = 0;

        try {
            if (numLeases == 0) {
                // If there are no leases, I shouldn't try to take any.
                return leasesToTake;
            }

            int target;
            if (numWorkers >= numLeases) {
                // If we have n leases and n or more workers, each worker can have up to 1 lease, including myself.
                target = 1;
            } else {
                /*
                 * if we have made it here, it means there are more leases than workers
                 *
                 * Our target for each worker is numLeases / numWorkers (+1 if numWorkers doesn't evenly divide numLeases)
                 */
                target = numLeases / numWorkers + (numLeases % numWorkers == 0 ? 0 : 1);

                // Spill over is the number of leases this worker should have claimed, but did not because it would
                // exceed the max allowed for this worker.
                leaseSpillover = Math.max(0, target - maxLeasesForWorker);
                if (target > maxLeasesForWorker) {
                    log.warn(
                            "Worker {} target is {} leases and maxLeasesForWorker is {}. Resetting target to {},"
                                    + " lease spillover is {}. Note that some shards may not be processed if no other "
                                    + "workers are able to pick them up.",
                            workerIdentifier,
                            target,
                            maxLeasesForWorker,
                            maxLeasesForWorker,
                            leaseSpillover);
                    target = maxLeasesForWorker;
                }
            }

            int myCount = leaseCounts.get(workerIdentifier);
            numLeasesToReachTarget = target - myCount;

            int currentLeaseCount = leaseCounts.get(workerIdentifier);
            // If there are leases that have been expired for an extended period of
            // time, take them with priority, disregarding the target (computed
            // later) but obeying the maximum limit per worker.
            if (enablePriorityLeaseAssignment) {
                long currentNanoTime;
                try {
                    currentNanoTime = timeProvider.call();
                } catch (Exception e) {
                    throw new DependencyException("Exception caught from timeProvider", e);
                }
                final long nanoThreshold = currentNanoTime - (veryOldLeaseDurationNanosMultiplier * leaseDurationNanos);
                final List<Lease> veryOldLeases = allLeases.values().stream()
                        .filter(lease -> nanoThreshold > lease.lastCounterIncrementNanos())
                        .collect(Collectors.toList());

                if (!veryOldLeases.isEmpty()) {
                    Collections.shuffle(veryOldLeases);
                    veryOldLeaseCount =
                            Math.max(0, Math.min(maxLeasesForWorker - currentLeaseCount, veryOldLeases.size()));
                    HashSet<Lease> result = new HashSet<>(veryOldLeases.subList(0, veryOldLeaseCount));
                    if (veryOldLeaseCount > 0) {
                        log.info("Taking leases that have been expired for a long time: {}", result);
                    }
                    return result;
                }
            }

            if (numLeasesToReachTarget <= 0) {
                // If we don't need anything, return the empty set.
                return leasesToTake;
            }

            // Shuffle availableLeases so workers don't all try to contend for the same leases.
            Collections.shuffle(availableLeases);

            if (availableLeases.size() > 0) {
                // If we have available leases, get up to <needed> leases from availableLeases
                for (; numLeasesToReachTarget > 0 && availableLeases.size() > 0; numLeasesToReachTarget--) {
                    leasesToTake.add(availableLeases.remove(0));
                }
            } else {
                // If there are no available leases and we need a lease, consider stealing.
                List<Lease> leasesToSteal = chooseLeasesToSteal(leaseCounts, numLeasesToReachTarget, target);
                for (Lease leaseToSteal : leasesToSteal) {
                    log.info(
                            "Worker {} needed {} leases but none were available, so it will steal lease {} from {}",
                            workerIdentifier,
                            numLeasesToReachTarget,
                            leaseToSteal.leaseKey(),
                            leaseToSteal.leaseOwner());
                    leasesToTake.add(leaseToSteal);
                }
            }

            if (!leasesToTake.isEmpty()) {
                log.info(
                        "Worker {} saw {} total leases, {} available leases, {} "
                                + "workers. Target is {} leases, I have {} leases, I will take {} leases",
                        workerIdentifier,
                        numLeases,
                        numAvailableLeases,
                        numWorkers,
                        target,
                        myCount,
                        leasesToTake.size());
            }
        } finally {
            scope.addData("ExpiredLeases", numAvailableLeases, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData("LeaseSpillover", leaseSpillover, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData("LeasesToTake", leasesToTake.size(), StandardUnit.COUNT, MetricsLevel.DETAILED);
            scope.addData(
                    "NeededLeases", Math.max(numLeasesToReachTarget, 0), StandardUnit.COUNT, MetricsLevel.DETAILED);
            scope.addData("NumWorkers", numWorkers, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData("TotalLeases", numLeases, StandardUnit.COUNT, MetricsLevel.DETAILED);
            scope.addData("VeryOldLeases", veryOldLeaseCount, StandardUnit.COUNT, MetricsLevel.SUMMARY);

            MetricsUtil.endScope(scope);
        }

        return leasesToTake;
    }

    /**
     * Choose leases to steal by randomly selecting one or more (up to max) from the most loaded worker.
     * Stealing rules:
     *
     * Steal up to maxLeasesToStealAtOneTime leases from the most loaded worker if
     * a) he has > target leases and I need >= 1 leases : steal min(leases needed, maxLeasesToStealAtOneTime)
     * b) he has == target leases and I need > 1 leases : steal 1
     *
     * @param leaseCounts map of workerIdentifier to lease count
     * @param needed # of leases needed to reach the target leases for the worker
     * @param target target # of leases per worker
     * @return Leases to steal, or empty list if we should not steal
     */
    private List<Lease> chooseLeasesToSteal(Map<String, Integer> leaseCounts, int needed, int target) {
        List<Lease> leasesToSteal = new ArrayList<>();

        Entry<String, Integer> mostLoadedWorker = null;
        // Find the most loaded worker
        for (Entry<String, Integer> worker : leaseCounts.entrySet()) {
            if (mostLoadedWorker == null || mostLoadedWorker.getValue() < worker.getValue()) {
                mostLoadedWorker = worker;
            }
        }

        int numLeasesToSteal = 0;
        if ((mostLoadedWorker.getValue() >= target) && (needed > 0)) {
            int leasesOverTarget = mostLoadedWorker.getValue() - target;
            numLeasesToSteal = Math.min(needed, leasesOverTarget);
            // steal 1 if we need > 1 and max loaded worker has target leases.
            if ((needed > 1) && (numLeasesToSteal == 0)) {
                numLeasesToSteal = 1;
            }
            numLeasesToSteal = Math.min(numLeasesToSteal, maxLeasesToStealAtOneTime);
        }

        if (numLeasesToSteal <= 0) {
            if (log.isDebugEnabled()) {
                log.debug(String.format(
                        "Worker %s not stealing from most loaded worker %s.  He has %d,"
                                + " target is %d, and I need %d",
                        workerIdentifier, mostLoadedWorker.getKey(), mostLoadedWorker.getValue(), target, needed));
            }
            return leasesToSteal;
        } else {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Worker {} will attempt to steal {} leases from most loaded worker {}. "
                                + " He has {} leases, target is {}, I need {}, maxLeasesToStealAtOneTime is {}.",
                        workerIdentifier,
                        numLeasesToSteal,
                        mostLoadedWorker.getKey(),
                        mostLoadedWorker.getValue(),
                        target,
                        needed,
                        maxLeasesToStealAtOneTime);
            }
        }

        String mostLoadedWorkerIdentifier = mostLoadedWorker.getKey();
        List<Lease> candidates = new ArrayList<>();
        // Collect leases belonging to that worker
        for (Lease lease : allLeases.values()) {
            if (mostLoadedWorkerIdentifier.equals(lease.leaseOwner())) {
                candidates.add(lease);
            }
        }

        // Return random ones
        Collections.shuffle(candidates);
        int toIndex = Math.min(candidates.size(), numLeasesToSteal);
        leasesToSteal.addAll(candidates.subList(0, toIndex).stream()
                .map(lease -> lease.isMarkedForLeaseSteal(true))
                .collect(Collectors.toList()));
        return leasesToSteal;
    }

    /**
     * Count leases by host. Always includes myself, but otherwise only includes hosts that are currently holding
     * leases.
     *
     * @param availableLeases list of leases that are currently available
     * @return map of workerIdentifier to lease count
     */
    @VisibleForTesting
    Map<String, Integer> computeLeaseCounts(List<Lease> availableLeases) {
        Map<String, Integer> leaseCounts = new HashMap<>();
        // The set will give much faster lookup than the original list, an
        // important optimization when the list is large
        Set<Lease> availableLeasesSet = new HashSet<>(availableLeases);

        // Compute the number of leases per worker by looking through allLeases and ignoring leases that are available.
        for (Lease lease : allLeases.values()) {
            if (!availableLeasesSet.contains(lease)) {
                String leaseOwner = lease.leaseOwner();
                Integer oldCount = leaseCounts.get(leaseOwner);
                if (oldCount == null) {
                    leaseCounts.put(leaseOwner, 1);
                } else {
                    leaseCounts.put(leaseOwner, oldCount + 1);
                }
            }
        }

        // If I have no leases, I wasn't represented in leaseCounts. Let's fix that.
        leaseCounts.putIfAbsent(workerIdentifier, 0);

        return leaseCounts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWorkerIdentifier() {
        return workerIdentifier;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized List<Lease> allLeases() {
        return new ArrayList<>(allLeases.values());
    }
}
