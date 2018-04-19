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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.metrics.IMetricsScope;
import software.amazon.kinesis.metrics.LogMetricsFactory;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.MetricsLevel;

/**
 * LeaseCoordinator abstracts away LeaseTaker and LeaseRenewer from the application code that's using leasing. It owns
 * the scheduling of the two previously mentioned components as well as informing LeaseRenewer when LeaseTaker takes new
 * leases.
 *
 */
@Slf4j
public class LeaseCoordinator<T extends Lease> {

    /*
     * Name of the dimension used when setting worker identifier on IMetricsScopes. Exposed so that users of this class
     * can easily create InterceptingMetricsFactories that rename this dimension to suit the destination metrics system.
     */
    public static final String WORKER_IDENTIFIER_METRIC = "WorkerIdentifier";

    // Time to wait for in-flight Runnables to finish when calling .stop();
    private static final long STOP_WAIT_TIME_MILLIS = 2000L;

    private static final int DEFAULT_MAX_LEASES_FOR_WORKER = Integer.MAX_VALUE;
    private static final int DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;

    private static final ThreadFactory LEASE_COORDINATOR_THREAD_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat("LeaseCoordinator-%04d").setDaemon(true).build();
    private static final ThreadFactory LEASE_RENEWAL_THREAD_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat("LeaseRenewer-%04d").setDaemon(true).build();

    private final LeaseRenewer<T> leaseRenewer;
    private final LeaseTaker<T> leaseTaker;
    private final long renewerIntervalMillis;
    private final long takerIntervalMillis;

    private final Object shutdownLock = new Object();

    protected final IMetricsFactory metricsFactory;

    private ScheduledExecutorService leaseCoordinatorThreadPool;
    private final ExecutorService leaseRenewalThreadpool;
    private volatile boolean running = false;
    private ScheduledFuture<?> takerFuture;

    /**
     * Constructor.
     *
     * @param leaseManager LeaseManager instance to use
     * @param workerIdentifier Identifies the worker (e.g. useful to track lease ownership)
     * @param leaseDurationMillis Duration of a lease
     * @param epsilonMillis Allow for some variance when calculating lease expirations
     */
    public LeaseCoordinator(LeaseManager<T> leaseManager,
            String workerIdentifier,
            long leaseDurationMillis,
            long epsilonMillis) {
        this(leaseManager, workerIdentifier, leaseDurationMillis, epsilonMillis, new LogMetricsFactory());
    }

    /**
     * Constructor.
     *
     * @param leaseManager LeaseManager instance to use
     * @param workerIdentifier Identifies the worker (e.g. useful to track lease ownership)
     * @param leaseDurationMillis Duration of a lease
     * @param epsilonMillis Allow for some variance when calculating lease expirations
     * @param metricsFactory Used to publish metrics about lease operations
     */
    public LeaseCoordinator(LeaseManager<T> leaseManager,
            String workerIdentifier,
            long leaseDurationMillis,
            long epsilonMillis,
            IMetricsFactory metricsFactory) {
        this(leaseManager, workerIdentifier, leaseDurationMillis, epsilonMillis,
                DEFAULT_MAX_LEASES_FOR_WORKER, DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME,
                LeaseManagementConfig.DEFAULT_MAX_LEASE_RENEWAL_THREADS, metricsFactory);
    }

    /**
     * Constructor.
     *
     * @param leaseManager LeaseManager instance to use
     * @param workerIdentifier Identifies the worker (e.g. useful to track lease ownership)
     * @param leaseDurationMillis Duration of a lease
     * @param epsilonMillis Allow for some variance when calculating lease expirations
     * @param maxLeasesForWorker Max leases this Worker can handle at a time
     * @param maxLeasesToStealAtOneTime Steal up to these many leases at a time (for load balancing)
     * @param metricsFactory Used to publish metrics about lease operations
     */
    public LeaseCoordinator(LeaseManager<T> leaseManager,
            String workerIdentifier,
            long leaseDurationMillis,
            long epsilonMillis,
            int maxLeasesForWorker,
            int maxLeasesToStealAtOneTime,
            int maxLeaseRenewerThreadCount,
            IMetricsFactory metricsFactory) {
        this.leaseRenewalThreadpool = getLeaseRenewalExecutorService(maxLeaseRenewerThreadCount);
        this.leaseTaker = new DynamoDBLeaseTaker<T>(leaseManager, workerIdentifier, leaseDurationMillis)
                .withMaxLeasesForWorker(maxLeasesForWorker)
                .withMaxLeasesToStealAtOneTime(maxLeasesToStealAtOneTime);
        this.leaseRenewer = new DynamoDBLeaseRenewer<T>(
                leaseManager, workerIdentifier, leaseDurationMillis, leaseRenewalThreadpool);
        this.renewerIntervalMillis = leaseDurationMillis / 3 - epsilonMillis;
        this.takerIntervalMillis = (leaseDurationMillis + epsilonMillis) * 2;
        this.metricsFactory = metricsFactory;

        log.info("With failover time {} ms and epsilon {} ms, LeaseCoordinator will renew leases every {} ms, take"
                        + "leases every {} ms, process maximum of {} leases and steal {} lease(s) at a time.",
                leaseDurationMillis,
                epsilonMillis,
                renewerIntervalMillis,
                takerIntervalMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime);
    }

    private class TakerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                runTaker();
            } catch (LeasingException e) {
                log.error("LeasingException encountered in lease taking thread", e);
            } catch (Throwable t) {
                log.error("Throwable encountered in lease taking thread", t);
            }
        }

    }

    private class RenewerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                runRenewer();
            } catch (LeasingException e) {
                log.error("LeasingException encountered in lease renewing thread", e);
            } catch (Throwable t) {
                log.error("Throwable encountered in lease renewing thread", t);
            }
        }

    }

    /**
     * Start background LeaseHolder and LeaseTaker threads.
     * @throws ProvisionedThroughputException If we can't talk to DynamoDB due to insufficient capacity.
     * @throws InvalidStateException If the lease table doesn't exist
     * @throws DependencyException If we encountered exception taking to DynamoDB
     */
    public void start() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        leaseRenewer.initialize();

        // 2 because we know we'll have at most 2 concurrent tasks at a time.
        leaseCoordinatorThreadPool = Executors.newScheduledThreadPool(2, LEASE_COORDINATOR_THREAD_FACTORY);

        // Taker runs with fixed DELAY because we want it to run slower in the event of performance degredation.
        takerFuture = leaseCoordinatorThreadPool.scheduleWithFixedDelay(new TakerRunnable(),
                0L,
                takerIntervalMillis,
                TimeUnit.MILLISECONDS);
        // Renewer runs at fixed INTERVAL because we want it to run at the same rate in the event of degredation.
        leaseCoordinatorThreadPool.scheduleAtFixedRate(new RenewerRunnable(),
                0L,
                renewerIntervalMillis,
                TimeUnit.MILLISECONDS);
        running = true;
    }

    /**
     * Runs a single iteration of the lease taker - used by integration tests.
     *
     * @throws InvalidStateException
     * @throws DependencyException
     */
    protected void runTaker() throws DependencyException, InvalidStateException {
        IMetricsScope scope = MetricsHelper.startScope(metricsFactory, "TakeLeases");
        long startTime = System.currentTimeMillis();
        boolean success = false;

        try {
            Map<String, T> takenLeases = leaseTaker.takeLeases();

            // Only add taken leases to renewer if coordinator is still running.
            synchronized (shutdownLock) {
                if (running) {
                    leaseRenewer.addLeasesToRenew(takenLeases.values());
                }
            }

            success = true;
        } finally {
            scope.addDimension(WORKER_IDENTIFIER_METRIC, getWorkerIdentifier());
            MetricsHelper.addSuccessAndLatency(startTime, success, MetricsLevel.SUMMARY);
            MetricsHelper.endScope();
        }
    }

    /**
     * Runs a single iteration of the lease renewer - used by integration tests.
     *
     * @throws InvalidStateException
     * @throws DependencyException
     */
    protected void runRenewer() throws DependencyException, InvalidStateException {
        IMetricsScope scope = MetricsHelper.startScope(metricsFactory, "RenewAllLeases");
        long startTime = System.currentTimeMillis();
        boolean success = false;

        try {
            leaseRenewer.renewLeases();
            success = true;
        } finally {
            scope.addDimension(WORKER_IDENTIFIER_METRIC, getWorkerIdentifier());
            MetricsHelper.addSuccessAndLatency(startTime, success, MetricsLevel.SUMMARY);
            MetricsHelper.endScope();
        }
    }

    /**
     * @return currently held leases
     */
    public Collection<T> getAssignments() {
        return leaseRenewer.getCurrentlyHeldLeases().values();
    }

    /**
     * @param leaseKey lease key to fetch currently held lease for
     *
     * @return deep copy of currently held Lease for given key, or null if we don't hold the lease for that key
     */
    public T getCurrentlyHeldLease(String leaseKey) {
        return leaseRenewer.getCurrentlyHeldLease(leaseKey);
    }

    /**
     * @return workerIdentifier
     */
    public String getWorkerIdentifier() {
        return leaseTaker.getWorkerIdentifier();
    }

    /**
     * Stops background threads and waits for {@link #STOP_WAIT_TIME_MILLIS} for all background tasks to complete.
     * If tasks are not completed after this time, method will shutdown thread pool forcefully and return.
     */
    public void stop() {
        if (leaseCoordinatorThreadPool != null) {
            leaseCoordinatorThreadPool.shutdown();
            try {
                if (leaseCoordinatorThreadPool.awaitTermination(STOP_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS)) {
                    log.info("Worker {} has successfully stopped lease-tracking threads",
                            leaseTaker.getWorkerIdentifier());
                } else {
                    leaseCoordinatorThreadPool.shutdownNow();
                    log.info("Worker {} stopped lease-tracking threads {} ms after stop",
                            leaseTaker.getWorkerIdentifier(),
                            STOP_WAIT_TIME_MILLIS);
                }
            } catch (InterruptedException e) {
                log.debug("Encountered InterruptedException when awaiting threadpool termination");
            }
        } else {
            log.debug("Threadpool was null, no need to shutdown/terminate threadpool.");
        }

        leaseRenewalThreadpool.shutdownNow();
        synchronized (shutdownLock) {
            leaseRenewer.clearCurrentlyHeldLeases();
            running = false;
        }
    }

    /**
     * Requests the cancellation of the lease taker.
     */
    public void stopLeaseTaker() {
        takerFuture.cancel(false);

    }

    /**
     * Requests that renewals for the given lease are stopped.
     * 
     * @param lease the lease to stop renewing.
     */
    public void dropLease(T lease) {
        synchronized (shutdownLock) {
            if (lease != null) {
                leaseRenewer.dropLease(lease);
            }
        }
    }

    /**
     * @return true if this LeaseCoordinator is running
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Updates application-specific lease values in DynamoDB.
     *
     * @param lease lease object containing updated values
     * @param concurrencyToken obtained by calling Lease.getConcurrencyToken for a currently held lease
     *
     * @return true if update succeeded, false otherwise
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    public boolean updateLease(T lease, UUID concurrencyToken)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return leaseRenewer.updateLease(lease, concurrencyToken);
    }

    /**
     * Returns executor service that should be used for lease renewal.
     * @param maximumPoolSize Maximum allowed thread pool size
     * @return Executor service that should be used for lease renewal.
     */
    private static ExecutorService getLeaseRenewalExecutorService(int maximumPoolSize) {
        int coreLeaseCount = Math.max(maximumPoolSize / 4, 2);

        return new ThreadPoolExecutor(coreLeaseCount, maximumPoolSize, 60, TimeUnit.SECONDS,
                new LinkedTransferQueue<Runnable>(), LEASE_RENEWAL_THREAD_FACTORY);
    }
}
