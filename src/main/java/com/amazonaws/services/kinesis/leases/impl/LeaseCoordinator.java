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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseRenewer;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseTaker;
import com.amazonaws.services.kinesis.metrics.impl.LogMetricsFactory;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;

/**
 * LeaseCoordinator abstracts away LeaseTaker and LeaseRenewer from the application code that's using leasing. It owns
 * the scheduling of the two previously mentioned components as well as informing LeaseRenewer when LeaseTaker takes new
 * leases.
 * 
 */
public class LeaseCoordinator<T extends Lease> {

    /*
     * Name of the dimension used when setting worker identifier on IMetricsScopes. Exposed so that users of this class
     * can easily create InterceptingMetricsFactories that rename this dimension to suit the destination metrics system.
     */
    public static final String WORKER_IDENTIFIER_METRIC = "WorkerIdentifier";

    private static final Log LOG = LogFactory.getLog(LeaseCoordinator.class);

    // Time to wait for in-flight Runnables to finish when calling .stop();
    private static final long STOP_WAIT_TIME_MILLIS = 2000L;

    private final ILeaseRenewer<T> leaseRenewer;
    private final ILeaseTaker<T> leaseTaker;
    private final long renewerIntervalMillis;
    private final long takerIntervalMillis;

    protected final IMetricsFactory metricsFactory;

    private ScheduledExecutorService threadpool;
    private boolean running = false;

    /**
     * Constructor.
     * 
     * @param leaseManager LeaseManager instance to use
     * @param workerIdentifier Identifies the worker (e.g. useful to track lease ownership)
     * @param leaseDurationMillis Duration of a lease
     * @param epsilonMillis Allow for some variance when calculating lease expirations
     */
    public LeaseCoordinator(ILeaseManager<T> leaseManager,
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
    public LeaseCoordinator(ILeaseManager<T> leaseManager,
            String workerIdentifier,
            long leaseDurationMillis,
            long epsilonMillis,
            IMetricsFactory metricsFactory) {
        this.leaseTaker = new LeaseTaker<T>(leaseManager, workerIdentifier, leaseDurationMillis);
        this.leaseRenewer = new LeaseRenewer<T>(leaseManager, workerIdentifier, leaseDurationMillis);
        this.renewerIntervalMillis = leaseDurationMillis / 3 - epsilonMillis;
        this.takerIntervalMillis = (leaseDurationMillis + epsilonMillis) * 2;
        this.metricsFactory = metricsFactory;

        LOG.info(String.format("With failover time %dms and epsilon %dms, LeaseCoordinator will renew leases every %dms and take leases every %dms",
                leaseDurationMillis,
                epsilonMillis,
                renewerIntervalMillis,
                takerIntervalMillis));
    }

    private class TakerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                runTaker();
            } catch (LeasingException e) {
                LOG.error("LeasingException encountered in lease taking thread", e);
            } catch (Throwable t) {
                LOG.error("Throwable encountered in lease taking thread", t);
            }
        }

    }

    private class RenewerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                runRenewer();
            } catch (LeasingException e) {
                LOG.error("LeasingException encountered in lease renewing thread", e);
            } catch (Throwable t) {
                LOG.error("Throwable encountered in lease renewing thread", t);
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
        threadpool = Executors.newScheduledThreadPool(2);

        // Taker runs with fixed DELAY because we want it to run slower in the event of performance degredation.
        threadpool.scheduleWithFixedDelay(new TakerRunnable(), 0L, takerIntervalMillis, TimeUnit.MILLISECONDS);
        // Renewer runs at fixed INTERVAL because we want it to run at the same rate in the event of degredation.
        threadpool.scheduleAtFixedRate(new RenewerRunnable(), 0L, renewerIntervalMillis, TimeUnit.MILLISECONDS);
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

            leaseRenewer.addLeasesToRenew(takenLeases.values());
            success = true;
        } finally {
            scope.addDimension(WORKER_IDENTIFIER_METRIC, getWorkerIdentifier());
            MetricsHelper.addSuccessAndLatency(startTime, success);
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
            MetricsHelper.addSuccessAndLatency(startTime, success);
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
     * Stops background threads.
     */
    public void stop() {
        threadpool.shutdown();
        try {
            if (threadpool.awaitTermination(STOP_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS)) {
                LOG.info(String.format("Worker %s has successfully stopped lease-tracking threads", leaseTaker.getWorkerIdentifier()));
            } else {
                threadpool.shutdownNow();
                LOG.info(String.format("Worker %s stopped lease-tracking threads %dms after stop",
                        leaseTaker.getWorkerIdentifier(),
                        STOP_WAIT_TIME_MILLIS));
            }
        } catch (InterruptedException e) {
            LOG.debug("Encountered InterruptedException when awaiting threadpool termination");
        }

        leaseRenewer.clearCurrentlyHeldLeases();
        running = false;
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

}
