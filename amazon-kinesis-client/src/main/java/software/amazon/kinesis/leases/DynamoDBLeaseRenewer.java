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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.ThreadSafeMetricsDelegatingScope;
import software.amazon.kinesis.metrics.IMetricsScope;
import software.amazon.kinesis.metrics.MetricsLevel;

import lombok.extern.slf4j.Slf4j;

/**
 * An implementation of ILeaseRenewer that uses DynamoDB via LeaseManager.
 */
@Slf4j
public class DynamoDBLeaseRenewer<T extends Lease> implements LeaseRenewer<T> {
    private static final int RENEWAL_RETRIES = 2;

    private final LeaseManager<T> leaseManager;
    private final ConcurrentNavigableMap<String, T> ownedLeases = new ConcurrentSkipListMap<String, T>();
    private final String workerIdentifier;
    private final long leaseDurationNanos;
    private final ExecutorService executorService;

    /**
     * Constructor.
     * 
     * @param leaseManager LeaseManager to use
     * @param workerIdentifier identifier of this worker
     * @param leaseDurationMillis duration of a lease in milliseconds
     * @param executorService ExecutorService to use for renewing leases in parallel
     */
    public DynamoDBLeaseRenewer(LeaseManager<T> leaseManager, String workerIdentifier, long leaseDurationMillis,
                                ExecutorService executorService) {
        this.leaseManager = leaseManager;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
        this.executorService = executorService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void renewLeases() throws DependencyException, InvalidStateException {
        if (log.isDebugEnabled()) {
            // Due to the eventually consistent nature of ConcurrentNavigableMap iterators, this log entry may become
            // inaccurate during iteration.
            log.debug("Worker {} holding %d leases: {}",
                    workerIdentifier,
                    ownedLeases.size(),
                    ownedLeases);
        }

        /*
         * Lease renewals are done in parallel so many leases can be renewed for short lease fail over time
         * configuration. In this case, metrics scope is also shared across different threads, so scope must be thread
         * safe.
         */
        IMetricsScope renewLeaseTaskMetricsScope = new ThreadSafeMetricsDelegatingScope(
                MetricsHelper.getMetricsScope());

        /*
         * We iterate in descending order here so that the synchronized(lease) inside renewLease doesn't "lead" calls
         * to getCurrentlyHeldLeases. They'll still cross paths, but they won't interleave their executions.
         */
        int lostLeases = 0;
        List<Future<Boolean>> renewLeaseTasks = new ArrayList<Future<Boolean>>();
        for (T lease : ownedLeases.descendingMap().values()) {
            renewLeaseTasks.add(executorService.submit(new RenewLeaseTask(lease, renewLeaseTaskMetricsScope)));
        }
        int leasesInUnknownState = 0;
        Exception lastException = null;
        for (Future<Boolean> renewLeaseTask : renewLeaseTasks) {
            try {
                if (!renewLeaseTask.get()) {
                    lostLeases++;
                }
            } catch (InterruptedException e) {
                log.info("Interrupted while waiting for a lease to renew.");
                leasesInUnknownState += 1;
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Encountered an exception while renewing a lease.", e.getCause());
                leasesInUnknownState += 1;
                lastException = e;
            }
        }

        renewLeaseTaskMetricsScope.addData(
                "LostLeases", lostLeases, StandardUnit.Count, MetricsLevel.SUMMARY);
        renewLeaseTaskMetricsScope.addData(
                "CurrentLeases", ownedLeases.size(), StandardUnit.Count, MetricsLevel.SUMMARY);
        if (leasesInUnknownState > 0) {
            throw new DependencyException(String.format("Encountered an exception while renewing leases. "
                    + "The number of leases which might not have been renewed is %d",
                    leasesInUnknownState),
                    lastException);
        }
    }

    private class RenewLeaseTask implements Callable<Boolean> {

        private final T lease;
        private final IMetricsScope metricsScope;

        public RenewLeaseTask(T lease, IMetricsScope metricsScope) {
            this.lease = lease;
            this.metricsScope = metricsScope;
        }

        @Override
        public Boolean call() throws Exception {
            MetricsHelper.setMetricsScope(metricsScope);
            try {
                return renewLease(lease);
            } finally {
                MetricsHelper.unsetMetricsScope();
            }
        }
    }

    private boolean renewLease(T lease) throws DependencyException, InvalidStateException {
        return renewLease(lease, false);
    }

    private boolean renewLease(T lease, boolean renewEvenIfExpired) throws DependencyException, InvalidStateException {
        String leaseKey = lease.getLeaseKey();

        boolean success = false;
        boolean renewedLease = false;
        long startTime = System.currentTimeMillis();
        try {
            for (int i = 1; i <= RENEWAL_RETRIES; i++) {
                try {
                    synchronized (lease) {
                        // Don't renew expired lease during regular renewals. getCopyOfHeldLease may have returned null
                        // triggering the application processing to treat this as a lost lease (fail checkpoint with
                        // ShutdownException).
                        boolean isLeaseExpired = lease.isExpired(leaseDurationNanos, System.nanoTime());
                        if (renewEvenIfExpired || !isLeaseExpired) {
                            renewedLease = leaseManager.renewLease(lease);
                        }
                        if (renewedLease) {
                            lease.setLastCounterIncrementNanos(System.nanoTime());
                        }
                    }

                    if (renewedLease) {
                        if (log.isDebugEnabled()) {
                            log.debug("Worker {} successfully renewed lease with key {}",
                                    workerIdentifier,
                                    leaseKey);
                        }
                    } else {
                        log.info("Worker {} lost lease with key {}", workerIdentifier, leaseKey);
                        ownedLeases.remove(leaseKey);
                    }

                    success = true;
                    break;
                } catch (ProvisionedThroughputException e) {
                    log.info("Worker {} could not renew lease with key {} on try {} out of {} due to capacity",
                            workerIdentifier,
                            leaseKey,
                            i,
                            RENEWAL_RETRIES);
                }
            }
        } finally {
            MetricsHelper.addSuccessAndLatency("RenewLease", startTime, success, MetricsLevel.DETAILED);
        }

        return renewedLease;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, T> getCurrentlyHeldLeases() {
        Map<String, T> result = new HashMap<String, T>();
        long now = System.nanoTime();

        for (String leaseKey : ownedLeases.keySet()) {
            T copy = getCopyOfHeldLease(leaseKey, now);
            if (copy != null) {
                result.put(copy.getLeaseKey(), copy);
            }
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getCurrentlyHeldLease(String leaseKey) {
        return getCopyOfHeldLease(leaseKey, System.nanoTime());
    }

    /**
     * Internal method to return a lease with a specific lease key only if we currently hold it.
     * 
     * @param leaseKey key of lease to return
     * @param now current timestamp for old-ness checking
     * @return non-authoritative copy of the held lease, or null if we don't currently hold it
     */
    private T getCopyOfHeldLease(String leaseKey, long now) {
        T authoritativeLease = ownedLeases.get(leaseKey);
        if (authoritativeLease == null) {
            return null;
        } else {
            T copy = null;
            synchronized (authoritativeLease) {
                copy = authoritativeLease.copy();
            }

            if (copy.isExpired(leaseDurationNanos, now)) {
                log.info("getCurrentlyHeldLease not returning lease with key {} because it is expired",
                        copy.getLeaseKey());
                return null;
            } else {
                return copy;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateLease(T lease, UUID concurrencyToken)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(lease, "lease cannot be null");
        verifyNotNull(lease.getLeaseKey(), "leaseKey cannot be null");
        verifyNotNull(concurrencyToken, "concurrencyToken cannot be null");

        String leaseKey = lease.getLeaseKey();
        T authoritativeLease = ownedLeases.get(leaseKey);

        if (authoritativeLease == null) {
            log.info("Worker {} could not update lease with key {} because it does not hold it",
                    workerIdentifier,
                    leaseKey);
            return false;
        }

        /*
         * If the passed-in concurrency token doesn't match the concurrency token of the authoritative lease, it means
         * the lease was lost and regained between when the caller acquired his concurrency token and when the caller
         * called update.
         */
        if (!authoritativeLease.getConcurrencyToken().equals(concurrencyToken)) {
            log.info("Worker {} refusing to update lease with key {} because"
                    + " concurrency tokens don't match", workerIdentifier, leaseKey);
            return false;
        }

        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            synchronized (authoritativeLease) {
                authoritativeLease.update(lease);
                boolean updatedLease = leaseManager.updateLease(authoritativeLease);
                if (updatedLease) {
                    // Updates increment the counter
                    authoritativeLease.setLastCounterIncrementNanos(System.nanoTime());
                } else {
                    /*
                     * If updateLease returns false, it means someone took the lease from us. Remove the lease
                     * from our set of owned leases pro-actively rather than waiting for a run of renewLeases().
                     */
                    log.info("Worker {} lost lease with key {} - discovered during update",
                            workerIdentifier,
                            leaseKey);

                    /*
                     * Remove only if the value currently in the map is the same as the authoritative lease. We're
                     * guarding against a pause after the concurrency token check above. It plays out like so:
                     * 
                     * 1) Concurrency token check passes
                     * 2) Pause. Lose lease, re-acquire lease. This requires at least one lease counter update.
                     * 3) Unpause. leaseManager.updateLease fails conditional write due to counter updates, returns
                     * false.
                     * 4) ownedLeases.remove(key, value) doesn't do anything because authoritativeLease does not
                     * .equals() the re-acquired version in the map on the basis of lease counter. This is what we want.
                     * If we just used ownedLease.remove(key), we would have pro-actively removed a lease incorrectly.
                     * 
                     * Note that there is a subtlety here - Lease.equals() deliberately does not check the concurrency
                     * token, but it does check the lease counter, so this scheme works.
                     */
                    ownedLeases.remove(leaseKey, authoritativeLease);
                }

                success = true;
                return updatedLease;
            }
        } finally {
            MetricsHelper.addSuccessAndLatency("UpdateLease", startTime, success, MetricsLevel.DETAILED);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addLeasesToRenew(Collection<T> newLeases) {
        verifyNotNull(newLeases, "newLeases cannot be null");

        for (T lease : newLeases) {
            if (lease.getLastCounterIncrementNanos() == null) {
                log.info("addLeasesToRenew ignoring lease with key {} because it does not have lastRenewalNanos set",
                        lease.getLeaseKey());
                continue;
            }

            T authoritativeLease = lease.copy();

            /*
             * Assign a concurrency token when we add this to the set of currently owned leases. This ensures that
             * every time we acquire a lease, it gets a new concurrency token.
             */
            authoritativeLease.setConcurrencyToken(UUID.randomUUID());
            ownedLeases.put(authoritativeLease.getLeaseKey(), authoritativeLease);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearCurrentlyHeldLeases() {
        ownedLeases.clear();
    }

    /**
     * {@inheritDoc}
     * @param lease the lease to drop.
     */
    @Override
    public void dropLease(T lease) {
        ownedLeases.remove(lease.getLeaseKey());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Collection<T> leases = leaseManager.listLeases();
        List<T> myLeases = new LinkedList<T>();
        boolean renewEvenIfExpired = true;

        for (T lease : leases) {
            if (workerIdentifier.equals(lease.getLeaseOwner())) {
                log.info(" Worker {} found lease {}", workerIdentifier, lease);
                // Okay to renew even if lease is expired, because we start with an empty list and we add the lease to
                // our list only after a successful renew. So we don't need to worry about the edge case where we could
                // continue renewing a lease after signaling a lease loss to the application.
                if (renewLease(lease, renewEvenIfExpired)) {
                    myLeases.add(lease);
                }
            } else {
                log.debug("Worker {} ignoring lease {} ", workerIdentifier, lease);
            }
        }

        addLeasesToRenew(myLeases);
    }

    private void verifyNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

}
