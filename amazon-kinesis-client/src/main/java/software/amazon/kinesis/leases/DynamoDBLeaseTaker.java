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
package software.amazon.kinesis.leases;

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

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.IMetricsScope;
import software.amazon.kinesis.metrics.MetricsLevel;

import lombok.extern.slf4j.Slf4j;

/**
 * An implementation of ILeaseTaker that uses DynamoDB via LeaseManager.
 */
@Slf4j
public class DynamoDBLeaseTaker<T extends Lease> implements LeaseTaker<T> {
    private static final int TAKE_RETRIES = 3;
    private static final int SCAN_RETRIES = 1;

    // See note on takeLeases(Callable) for why we have this callable.
    private static final Callable<Long> SYSTEM_CLOCK_CALLABLE = new Callable<Long>() {

        @Override
        public Long call() {
            return System.nanoTime();
        }
    };

    private final LeaseManager<T> leaseManager;
    private final String workerIdentifier;
    private final Map<String, T> allLeases = new HashMap<String, T>();
    private final long leaseDurationNanos;
    private int maxLeasesForWorker = Integer.MAX_VALUE;
    private int maxLeasesToStealAtOneTime = 1;

    private long lastScanTimeNanos = 0L;

    public DynamoDBLeaseTaker(LeaseManager<T> leaseManager, String workerIdentifier, long leaseDurationMillis) {
        this.leaseManager = leaseManager;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
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
    public DynamoDBLeaseTaker<T> withMaxLeasesForWorker(int maxLeasesForWorker) {
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
    public DynamoDBLeaseTaker<T> withMaxLeasesToStealAtOneTime(int maxLeasesToStealAtOneTime) {
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
    public Map<String, T> takeLeases() throws DependencyException, InvalidStateException {
        return takeLeases(SYSTEM_CLOCK_CALLABLE);
    }

    /**
     * Internal implementation of takeLeases. Takes a callable that can provide the time to enable test cases without
     * Thread.sleep. Takes a callable instead of a raw time value because the time needs to be computed as-of
     * immediately after the scan.
     * 
     * @param timeProvider Callable that will supply the time
     * 
     * @return map of lease key to taken lease
     * 
     * @throws DependencyException
     * @throws InvalidStateException
     */
    synchronized Map<String, T> takeLeases(Callable<Long> timeProvider)
        throws DependencyException, InvalidStateException {
        // Key is leaseKey
        Map<String, T> takenLeases = new HashMap<String, T>();

        long startTime = System.currentTimeMillis();
        boolean success = false;

        ProvisionedThroughputException lastException = null;

        try {
            for (int i = 1; i <= SCAN_RETRIES; i++) {
                try {
                    updateAllLeases(timeProvider);
                    success = true;
                } catch (ProvisionedThroughputException e) {
                    log.info("Worker {} could not find expired leases on try {} out of {}",
                            workerIdentifier,
                            i,
                            TAKE_RETRIES);
                    lastException = e;
                }
            }
        } finally {
            MetricsHelper.addSuccessAndLatency("ListLeases", startTime, success, MetricsLevel.DETAILED);
        }

        if (lastException != null) {
            log.error("Worker {} could not scan leases table, aborting takeLeases. Exception caught by last retry:",
                    workerIdentifier,
                    lastException);
            return takenLeases;
        }

        List<T> expiredLeases = getExpiredLeases();

        Set<T> leasesToTake = computeLeasesToTake(expiredLeases);
        Set<String> untakenLeaseKeys = new HashSet<String>();

        for (T lease : leasesToTake) {
            String leaseKey = lease.getLeaseKey();

            startTime = System.currentTimeMillis();
            success = false;
            try {
                for (int i = 1; i <= TAKE_RETRIES; i++) {
                    try {
                        if (leaseManager.takeLease(lease, workerIdentifier)) {
                            lease.setLastCounterIncrementNanos(System.nanoTime());
                            takenLeases.put(leaseKey, lease);
                        } else {
                            untakenLeaseKeys.add(leaseKey);
                        }

                        success = true;
                        break;
                    } catch (ProvisionedThroughputException e) {
                        log.info("Could not take lease with key {} for worker {} on try {} out of {} due to capacity",
                                leaseKey,
                                workerIdentifier,
                                i,
                                TAKE_RETRIES);
                    }
                }
            } finally {
                MetricsHelper.addSuccessAndLatency("TakeLease", startTime, success, MetricsLevel.DETAILED);
            }
        }

        if (takenLeases.size() > 0) {
            log.info("Worker {} successfully took {} leases: {}",
                    workerIdentifier,
                    takenLeases.size(),
                    stringJoin(takenLeases.keySet(), ", "));
        }

        if (untakenLeaseKeys.size() > 0) {
            log.info("Worker {} failed to take {} leases: {}",
                    workerIdentifier,
                    untakenLeaseKeys.size(),
                    stringJoin(untakenLeaseKeys, ", "));
        }

        MetricsHelper.getMetricsScope().addData(
                "TakenLeases", takenLeases.size(), StandardUnit.Count, MetricsLevel.SUMMARY);

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
        List<T> freshList = leaseManager.listLeases();
        try {
            lastScanTimeNanos = timeProvider.call();
        } catch (Exception e) {
            throw new DependencyException("Exception caught from timeProvider", e);
        }

        // This set will hold the lease keys not updated by the previous listLeases call.
        Set<String> notUpdated = new HashSet<String>(allLeases.keySet());

        // Iterate over all leases, finding ones to try to acquire that haven't changed since the last iteration
        for (T lease : freshList) {
            String leaseKey = lease.getLeaseKey();

            T oldLease = allLeases.get(leaseKey);
            allLeases.put(leaseKey, lease);
            notUpdated.remove(leaseKey);

            if (oldLease != null) {
                // If we've seen this lease before...
                if (oldLease.getLeaseCounter().equals(lease.getLeaseCounter())) {
                    // ...and the counter hasn't changed, propagate the lastRenewalNanos time from the old lease
                    lease.setLastCounterIncrementNanos(oldLease.getLastCounterIncrementNanos());
                } else {
                    // ...and the counter has changed, set lastRenewalNanos to the time of the scan.
                    lease.setLastCounterIncrementNanos(lastScanTimeNanos);
                }
            } else {
                if (lease.getLeaseOwner() == null) {
                    // if this new lease is unowned, it's never been renewed.
                    lease.setLastCounterIncrementNanos(0L);

                    if (log.isDebugEnabled()) {
                        log.debug("Treating new lease with key {} as never renewed because it is new and unowned.",
                                leaseKey);
                    }
                } else {
                    // if this new lease is owned, treat it as renewed as of the scan
                    lease.setLastCounterIncrementNanos(lastScanTimeNanos);
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
    private List<T> getExpiredLeases() {
        List<T> expiredLeases = new ArrayList<T>();

        for (T lease : allLeases.values()) {
            if (lease.isExpired(leaseDurationNanos, lastScanTimeNanos)) {
                expiredLeases.add(lease);
            }
        }

        return expiredLeases;
    }

    /**
     * Compute the number of leases I should try to take based on the state of the system.
     * 
     * @param allLeases map of shardId to lease containing all leases
     * @param expiredLeases list of leases we determined to be expired
     * @return set of leases to take.
     */
    private Set<T> computeLeasesToTake(List<T> expiredLeases) {
        Map<String, Integer> leaseCounts = computeLeaseCounts(expiredLeases);
        Set<T> leasesToTake = new HashSet<T>();
        IMetricsScope metrics = MetricsHelper.getMetricsScope();

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
                log.warn("Worker {} target is {} leases and maxLeasesForWorker is {}."
                        + " Resetting target to {}, lease spillover is {}. "
                        + " Note that some shards may not be processed if no other workers are able to pick them up.",
                        workerIdentifier,
                        target,
                        maxLeasesForWorker,
                        maxLeasesForWorker,
                        leaseSpillover);
                target = maxLeasesForWorker;
            }
            metrics.addData("LeaseSpillover", leaseSpillover, StandardUnit.Count, MetricsLevel.SUMMARY);
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
            List<T> leasesToSteal = chooseLeasesToSteal(leaseCounts, numLeasesToReachTarget, target);
            for (T leaseToSteal : leasesToSteal) {
                log.info("Worker {} needed {} leases but none were expired, so it will steal lease {} from {}",
                        workerIdentifier,
                        numLeasesToReachTarget,
                        leaseToSteal.getLeaseKey(),
                        leaseToSteal.getLeaseOwner());
                leasesToTake.add(leaseToSteal);
            }
        }

        if (!leasesToTake.isEmpty()) {
            log.info("Worker {} saw {} total leases, {} available leases, {} "
                    + "workers. Target is {} leases, I have {} leases, I will take {} leases",
                    workerIdentifier,
                    numLeases,
                    originalExpiredLeasesSize,
                    numWorkers,
                    target,
                    myCount,
                    leasesToTake.size());
        }

        metrics.addData("TotalLeases", numLeases, StandardUnit.Count, MetricsLevel.DETAILED);
        metrics.addData("ExpiredLeases", originalExpiredLeasesSize, StandardUnit.Count, MetricsLevel.SUMMARY);
        metrics.addData("NumWorkers", numWorkers, StandardUnit.Count, MetricsLevel.SUMMARY);
        metrics.addData("NeededLeases", numLeasesToReachTarget, StandardUnit.Count, MetricsLevel.DETAILED);
        metrics.addData("LeasesToTake", leasesToTake.size(), StandardUnit.Count, MetricsLevel.DETAILED);

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
    private List<T> chooseLeasesToSteal(Map<String, Integer> leaseCounts, int needed, int target) {
        List<T> leasesToSteal = new ArrayList<>();

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
        List<T> candidates = new ArrayList<T>();
        // Collect leases belonging to that worker
        for (T lease : allLeases.values()) {
            if (mostLoadedWorkerIdentifier.equals(lease.getLeaseOwner())) {
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
    private Map<String, Integer> computeLeaseCounts(List<T> expiredLeases) {
        Map<String, Integer> leaseCounts = new HashMap<String, Integer>();

        // Compute the number of leases per worker by looking through allLeases and ignoring leases that have expired.
        for (T lease : allLeases.values()) {
            if (!expiredLeases.contains(lease)) {
                String leaseOwner = lease.getLeaseOwner();
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
}
