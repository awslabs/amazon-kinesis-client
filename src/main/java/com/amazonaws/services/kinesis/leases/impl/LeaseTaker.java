/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.leases.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseTaker;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;

/**
 * An implementation of ILeaseTaker that uses DynamoDB via LeaseManager.
 */
public class LeaseTaker<T extends Lease> implements ILeaseTaker<T> {

    private static final Log LOG = LogFactory.getLog(LeaseTaker.class);

    private static final int TAKE_RETRIES = 3;
    private static final int SCAN_RETRIES = 1;

    // See note on takeLeases(Callable) for why we have this callable.
    private static final Callable<Long> SYSTEM_CLOCK_CALLABLE = new Callable<Long>() {

        @Override
        public Long call() {
            return System.nanoTime();
        }
    };

    private final ILeaseManager<T> leaseManager;
    private final String workerIdentifier;
    private final Map<String, T> allLeases = new HashMap<String, T>();
    private final long leaseDurationNanos;

    private Random random = new Random();
    private long lastScanTimeNanos = 0L;

    public LeaseTaker(ILeaseManager<T> leaseManager, String workerIdentifier, long leaseDurationMillis) {
        this.leaseManager = leaseManager;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationNanos = leaseDurationMillis * 1000000;
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
                    LOG.info(String.format("Worker %s could not find expired leases on try %d out of %d",
                            workerIdentifier,
                            i,
                            TAKE_RETRIES));
                    lastException = e;
                }
            }
        } finally {
            MetricsHelper.addSuccessAndLatency("ListLeases", startTime, success);
        }

        if (lastException != null) {
            LOG.error("Worker " + workerIdentifier
                    + " could not scan leases table, aborting takeLeases. Exception caught by last retry:",
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
                        LOG.info(String.format("Could not take lease with key %s for worker %s on try %d out of %d due to capacity",
                                leaseKey,
                                workerIdentifier,
                                i,
                                TAKE_RETRIES));
                    }
                }
            } finally {
                MetricsHelper.addSuccessAndLatency("TakeLease", startTime, success);
            }
        }

        if (takenLeases.size() > 0) {
            LOG.info(String.format("Worker %s successfully took %d leases: %s",
                    workerIdentifier,
                    takenLeases.size(),
                    stringJoin(takenLeases.keySet(), ", ")));
        }

        if (untakenLeaseKeys.size() > 0) {
            LOG.info(String.format("Worker %s failed to take %d leases: %s",
                    workerIdentifier,
                    untakenLeaseKeys.size(),
                    stringJoin(untakenLeaseKeys, ", ")));
        }

        MetricsHelper.getMetricsScope().addData("TakenLeases", takenLeases.size(), StandardUnit.Count);

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

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Treating new lease with key " + leaseKey
                                + " as never renewed because it is new and unowned.");
                    }
                } else {
                    // if this new lease is owned, treat it as renewed as of the scan
                    lease.setLastCounterIncrementNanos(lastScanTimeNanos);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Treating new lease with key " + leaseKey
                                + " as recently renewed because it is new and owned.");
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
            // If there are no expired leases and we need a lease, consider stealing one
            T leaseToSteal = chooseLeaseToSteal(leaseCounts, numLeasesToReachTarget, target);
            if (leaseToSteal != null) {
                LOG.info(String.format("Worker %s needed %d leases but none were expired, so it will steal lease %s from %s",
                        workerIdentifier,
                        numLeasesToReachTarget,
                        leaseToSteal.getLeaseKey(),
                        leaseToSteal.getLeaseOwner()));
                leasesToTake.add(leaseToSteal);
            }
        }

        if (!leasesToTake.isEmpty()) {
            LOG.info(String.format("Worker %s saw %d total leases, %d available leases, %d "
                    + "workers. Target is %d leases, I have %d leases, I will take %d leases",
                    workerIdentifier,
                    numLeases,
                    originalExpiredLeasesSize,
                    numWorkers,
                    target,
                    myCount,
                    leasesToTake.size()));
        }
        
        IMetricsScope metrics = MetricsHelper.getMetricsScope();
        metrics.addData("TotalLeases", numLeases, StandardUnit.Count);
        metrics.addData("ExpiredLeases", originalExpiredLeasesSize, StandardUnit.Count);
        metrics.addData("NumWorkers", numWorkers, StandardUnit.Count);
        metrics.addData("NeededLeases", numLeasesToReachTarget, StandardUnit.Count);
        metrics.addData("LeasesToTake", leasesToTake.size(), StandardUnit.Count);

        return leasesToTake;
    }

    /**
     * Choose a lease to steal by randomly selecting one from the most loaded worker. Stealing rules:
     * 
     * Steal one lease from the most loaded worker if
     * a) he has > target leases and I need >= 1 leases
     * b) he has == target leases and I need > 1 leases
     * 
     * @param leaseCounts map of workerIdentifier to lease count
     * @param target target # of leases per worker
     * @return Lease to steal, or null if we should not steal
     */
    private T chooseLeaseToSteal(Map<String, Integer> leaseCounts, int needed, int target) {
        Entry<String, Integer> mostLoadedWorker = null;
        // Find the most loaded worker
        for (Entry<String, Integer> worker : leaseCounts.entrySet()) {
            if (mostLoadedWorker == null || mostLoadedWorker.getValue() < worker.getValue()) {
                mostLoadedWorker = worker;
            }
        }

        if (mostLoadedWorker.getValue() < target + (needed > 1 ? 0 : 1)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Worker %s not stealing from most loaded worker %s.  He has %d,"
                        + " target is %d, and I need %d",
                        workerIdentifier,
                        mostLoadedWorker.getKey(),
                        mostLoadedWorker.getValue(),
                        target,
                        needed));
            }

            return null;
        }

        String mostLoadedWorkerIdentifier = mostLoadedWorker.getKey();
        List<T> candidates = new ArrayList<T>();
        // Collect leases belonging to that worker
        for (T lease : allLeases.values()) {
            if (mostLoadedWorkerIdentifier.equals(lease.getLeaseOwner())) {
                candidates.add(lease);
            }
        }

        // Return a random one
        int randomIndex = random.nextInt(candidates.size());
        return candidates.get(randomIndex);
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
