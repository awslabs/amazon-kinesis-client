/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
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
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsUtil;

/**
 * An implementation of {@link LeaseTaker} that uses DynamoDB via {@link LeaseRefresher}.
 */
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLeaseTaker implements LeaseTaker {
    private static final int TAKE_RETRIES = 3;
    private static final int SCAN_RETRIES = 1;

    // See note on TAKE_LEASES_DIMENSION(Callable) for why we have this callable.
    private static final Callable<Long> SYSTEM_CLOCK_CALLABLE = System::nanoTime;

    private static final String TAKE_LEASES_DIMENSION = "TakeLeases";

    private final LeaseRefresher leaseRefresher;
    private final String workerIdentifier;
    private final long leaseDurationNanos;
    private final MetricsFactory metricsFactory;

    private final Map<String, Lease> allLeases = new HashMap<>();
    // TODO: Remove these defaults and use the defaults in the config
    private int maxLeasesForWorker = Integer.MAX_VALUE;
    private int maxLeasesToStealAtOneTime = 1;

    private long lastScanTimeNanos = 0L;

    public DynamoDBLeaseTaker(LeaseRefresher leaseRefresher, String workerIdentifier, long leaseDurationMillis,
            final MetricsFactory metricsFactory) {
        this.leaseRefresher = leaseRefresher;
        this.workerIdentifier = workerIdentifier;
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
        boolean success = false;

        ProvisionedThroughputException lastException = null;

        try {
            try {
                for (int i = 1; i <= SCAN_RETRIES; i++) {
                    try {
                        updateAllLeases(timeProvider);
                        success = true;
                    } catch (ProvisionedThroughputException e) {
                        log.info("Worker {} could not find expired leases on try {} out of {}", workerIdentifier, i,
                                TAKE_RETRIES);
                        lastException = e;
                    }
                }
            } finally {
                MetricsUtil.addWorkerIdentifier(scope, workerIdentifier);
                MetricsUtil.addSuccessAndLatency(scope, "ListLeases", success, startTime, MetricsLevel.DETAILED);
            }

            if (lastException != null) {
                log.error("Worker {} could not scan leases table, aborting TAKE_LEASES_DIMENSION. Exception caught by"
                                + " last retry:", workerIdentifier, lastException);
                return takenLeases;
            }

            List<Lease> expiredLeases = getExpiredLeases();

            Set<Lease> leasesToTake = computeLeasesToTake(expiredLeases);
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
                            log.info("Could not take lease with key {} for worker {} on try {} out of {} due to"
                                            + " capacity", leaseKey, workerIdentifier, i, TAKE_RETRIES);
                        }
                    }
                } finally {
                    MetricsUtil.addSuccessAndLatency(scope, "TakeLease", success, startTime, MetricsLevel.DETAILED);
                }
            }

            if (takenLeases.size() > 0) {
                log.info("Worker {} successfully took {} leases: {}", workerIdentifier, takenLeases.size(),
                        stringJoin(takenLeases.keySet(), ", "));
            }

            if (untakenLeaseKeys.size() > 0) {
                log.info("Worker {} failed to take {} leases: {}", workerIdentifier, untakenLeaseKeys.size(),
                        stringJoin(untakenLeaseKeys, ", "));
            }

            scope.addData("TakenLeases", takenLeases.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
        } finally {
            MetricsUtil.endScope(scope);
        }

        return takenLeases;
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
     * @return list of expired leases, possibly empty, never null.
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

            Lease oldLease = allLeases.get(leaseKey);
            allLeases.put(leaseKey, lease);
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
                        log.debug("Treating new lease with key {} as never renewed because it is new and unowned.",
                                leaseKey);
                    }
                } else {
                    // if this new lease is owned, treat it as renewed as of the scan
                    lease.lastCounterIncrementNanos(lastScanTimeNanos);
                    if (log.isDebugEnabled()) {
                        log.debug("Treating new lease with key {} as recently renewed because it is new and owned.",
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
     * @return list of leases that were expired as of our last scan.
     */
    private List<Lease> getExpiredLeases() {
        List<Lease> expiredLeases = new ArrayList<>();

        for (Lease lease : allLeases.values()) {
            if (lease.isExpired(leaseDurationNanos, lastScanTimeNanos)) {
                expiredLeases.add(lease);
            }
        }

        return expiredLeases;
    }

    /**
     * Compute the number of leases I should try to take based on the state of the system.
     * 
     * @param expiredLeases list of leases we determined to be expired
     * @return set of leases to take.
     */
    private Set<Lease> computeLeasesToTake(List<Lease> expiredLeases) {
        Map<String, Integer> leaseCounts = computeLeaseCounts(expiredLeases);
        Set<Lease> leasesToTake = new HashSet<>();
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, TAKE_LEASES_DIMENSION);
        MetricsUtil.addWorkerIdentifier(scope, workerIdentifier);

        try {
            int numLeases = allLeases.size();
            int numWorkers = leaseCounts.size();

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
             * numWorkers must be < numLeases.
             * 
             * Our target for each worker is numLeases / numWorkers (+1 if numWorkers doesn't evenly divide numLeases)
             */
                target = numLeases / numWorkers + (numLeases % numWorkers == 0 ? 0 : 1);

                // Spill over is the number of leases this worker should have claimed, but did not because it would
                // exceed the max allowed for this worker.
                int leaseSpillover = Math.max(0, target - maxLeasesForWorker);
                if (target > maxLeasesForWorker) {
                    log.warn(
                            "Worker {} target is {} leases and maxLeasesForWorker is {}. Resetting target to {},"
                                    + " lease spillover is {}. Note that some shards may not be processed if no other "
                                    + "workers are able to pick them up.",
                            workerIdentifier, target, maxLeasesForWorker, maxLeasesForWorker, leaseSpillover);
                    target = maxLeasesForWorker;
                }
                scope.addData("LeaseSpillover", leaseSpillover, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            }

            int myCount = leaseCounts.get(workerIdentifier);
            int numLeasesToReachTarget = target - myCount;

            if (numLeasesToReachTarget <= 0) {
                // If we don't need anything, return the empty set.
                return leasesToTake;
            }

            // Shuffle expiredLeases so workers don't all try to contend for the same leases.
            Collections.shuffle(expiredLeases);

            int originalExpiredLeasesSize = expiredLeases.size();
            if (expiredLeases.size() > 0) {
                // If we have expired leases, get up to <needed> leases from expiredLeases
                for (; numLeasesToReachTarget > 0 && expiredLeases.size() > 0; numLeasesToReachTarget--) {
                    leasesToTake.add(expiredLeases.remove(0));
                }
            } else {
                // If there are no expired leases and we need a lease, consider stealing.
                List<Lease> leasesToSteal = chooseLeasesToSteal(leaseCounts, numLeasesToReachTarget, target);
                for (Lease leaseToSteal : leasesToSteal) {
                    log.info("Worker {} needed {} leases but none were expired, so it will steal lease {} from {}",
                            workerIdentifier, numLeasesToReachTarget, leaseToSteal.leaseKey(),
                            leaseToSteal.leaseOwner());
                    leasesToTake.add(leaseToSteal);
                }
            }

            if (!leasesToTake.isEmpty()) {
                log.info(
                        "Worker {} saw {} total leases, {} available leases, {} "
                                + "workers. Target is {} leases, I have {} leases, I will take {} leases",
                        workerIdentifier, numLeases, originalExpiredLeasesSize, numWorkers, target, myCount,
                        leasesToTake.size());
            }

            scope.addData("TotalLeases", numLeases, StandardUnit.COUNT, MetricsLevel.DETAILED);
            scope.addData("ExpiredLeases", originalExpiredLeasesSize, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData("NumWorkers", numWorkers, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData("NeededLeases", numLeasesToReachTarget, StandardUnit.COUNT, MetricsLevel.DETAILED);
            scope.addData("LeasesToTake", leasesToTake.size(), StandardUnit.COUNT, MetricsLevel.DETAILED);
        } finally {
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
                log.debug(String.format("Worker %s not stealing from most loaded worker %s.  He has %d,"
                        + " target is %d, and I need %d",
                        workerIdentifier,
                        mostLoadedWorker.getKey(),
                        mostLoadedWorker.getValue(),
                        target,
                        needed));
            }
            return leasesToSteal;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Worker {} will attempt to steal {} leases from most loaded worker {}. "
                        + " He has {} leases, target is {}, I need {}, maxLeasesToSteatAtOneTime is {}.",
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
        leasesToSteal.addAll(candidates.subList(0, toIndex));

        return leasesToSteal;
    }

    /**
     * Count leases by host. Always includes myself, but otherwise only includes hosts that are currently holding
     * leases.
     * 
     * @param expiredLeases list of leases that are currently expired
     * @return map of workerIdentifier to lease count
     */
    private Map<String, Integer> computeLeaseCounts(List<Lease> expiredLeases) {
        Map<String, Integer> leaseCounts = new HashMap<>();

        // Compute the number of leases per worker by looking through allLeases and ignoring leases that have expired.
        for (Lease lease : allLeases.values()) {
            if (!expiredLeases.contains(lease)) {
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
        Integer myCount = leaseCounts.get(workerIdentifier);
        if (myCount == null) {
            myCount = 0;
            leaseCounts.put(workerIdentifier, myCount);
        }

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
