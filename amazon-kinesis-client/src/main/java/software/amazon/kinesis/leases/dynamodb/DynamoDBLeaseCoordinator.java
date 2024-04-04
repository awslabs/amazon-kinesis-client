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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Collection;
import java.util.List;
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
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseRenewer;
import software.amazon.kinesis.leases.LeaseTaker;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static software.amazon.kinesis.common.CommonCalculations.getRenewerTakerIntervalMillis;

/**
 * LeaseCoordinator abstracts away LeaseTaker and LeaseRenewer from the application code that's using leasing. It owns
 * the scheduling of the two previously mentioned components as well as informing LeaseRenewer when LeaseTaker takes new
 * leases.
 *
 */
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLeaseCoordinator implements LeaseCoordinator {
    // Time to wait for in-flight Runnables to finish when calling .stop();
    private static final long STOP_WAIT_TIME_MILLIS = 2000L;
    private static final ThreadFactory LEASE_COORDINATOR_THREAD_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat("LeaseCoordinator-%04d").setDaemon(true).build();
    private static final ThreadFactory LEASE_RENEWAL_THREAD_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat("LeaseRenewer-%04d").setDaemon(true).build();

    private final LeaseRenewer leaseRenewer;
    private final LeaseTaker leaseTaker;
    private final long renewerIntervalMillis;
    private final long takerIntervalMillis;
    private final ExecutorService leaseRenewalThreadpool;
    private final LeaseRefresher leaseRefresher;
    private long initialLeaseTableReadCapacity;
    private long initialLeaseTableWriteCapacity;
    protected final MetricsFactory metricsFactory;

    private final Object shutdownLock = new Object();

    private ScheduledExecutorService leaseCoordinatorThreadPool;
    private ScheduledFuture<?> takerFuture;

    private volatile boolean running = false;

    /**
     * Constructor.
     *
     * <p>NOTE: This constructor is deprecated and will be removed in a future release.</p>
     *
     * @param leaseRefresher
     *            LeaseRefresher instance to use
     * @param workerIdentifier
     *            Identifies the worker (e.g. useful to track lease ownership)
     * @param leaseDurationMillis
     *            Duration of a lease
     * @param epsilonMillis
     *            Allow for some variance when calculating lease expirations
     * @param maxLeasesForWorker
     *            Max leases this Worker can handle at a time
     * @param maxLeasesToStealAtOneTime
     *            Steal up to these many leases at a time (for load balancing)
     * @param metricsFactory
     *            Used to publish metrics about lease operations
     */
    @Deprecated
    public DynamoDBLeaseCoordinator(final LeaseRefresher leaseRefresher,
                                    final String workerIdentifier,
                                    final long leaseDurationMillis,
                                    final long epsilonMillis,
                                    final int maxLeasesForWorker,
                                    final int maxLeasesToStealAtOneTime,
                                    final int maxLeaseRenewerThreadCount,
                                    final MetricsFactory metricsFactory) {
        this(leaseRefresher, workerIdentifier, leaseDurationMillis, epsilonMillis, maxLeasesForWorker,
                maxLeasesToStealAtOneTime, maxLeaseRenewerThreadCount,
                TableConstants.DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY,
                TableConstants.DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY, metricsFactory);
    }

    /**
     * Constructor.
     *
     * @param leaseRefresher
     *            LeaseRefresher instance to use
     * @param workerIdentifier
     *            Identifies the worker (e.g. useful to track lease ownership)
     * @param leaseDurationMillis
     *            Duration of a lease
     * @param epsilonMillis
     *            Allow for some variance when calculating lease expirations
     * @param maxLeasesForWorker
     *            Max leases this Worker can handle at a time
     * @param maxLeasesToStealAtOneTime
     *            Steal up to these many leases at a time (for load balancing)
     * @param initialLeaseTableReadCapacity
     *            Initial dynamodb lease table read iops if creating the lease table
     * @param initialLeaseTableWriteCapacity
     *            Initial dynamodb lease table write iops if creating the lease table
     * @param metricsFactory
     *            Used to publish metrics about lease operations
     */
    public DynamoDBLeaseCoordinator(final LeaseRefresher leaseRefresher,
                                    final String workerIdentifier,
                                    final long leaseDurationMillis,
                                    final long epsilonMillis,
                                    final int maxLeasesForWorker,
                                    final int maxLeasesToStealAtOneTime,
                                    final int maxLeaseRenewerThreadCount,
                                    final long initialLeaseTableReadCapacity,
                                    final long initialLeaseTableWriteCapacity,
                                    final MetricsFactory metricsFactory) {
        this.leaseRefresher = leaseRefresher;
        this.leaseRenewalThreadpool = getLeaseRenewalExecutorService(maxLeaseRenewerThreadCount);
        this.leaseTaker = new DynamoDBLeaseTaker(leaseRefresher, workerIdentifier, leaseDurationMillis, metricsFactory)
                .withMaxLeasesForWorker(maxLeasesForWorker)
                .withMaxLeasesToStealAtOneTime(maxLeasesToStealAtOneTime);
        this.leaseRenewer = new DynamoDBLeaseRenewer(
                leaseRefresher, workerIdentifier, leaseDurationMillis, leaseRenewalThreadpool, metricsFactory);
        this.renewerIntervalMillis = getRenewerTakerIntervalMillis(leaseDurationMillis, epsilonMillis);
        this.takerIntervalMillis = (leaseDurationMillis + epsilonMillis) * 2;
        if (initialLeaseTableReadCapacity <= 0) {
            throw new IllegalArgumentException("readCapacity should be >= 1");
        }
        this.initialLeaseTableReadCapacity = initialLeaseTableReadCapacity;
        if (initialLeaseTableWriteCapacity <= 0) {
            throw new IllegalArgumentException("writeCapacity should be >= 1");
        }
        this.initialLeaseTableWriteCapacity = initialLeaseTableWriteCapacity;
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
                runLeaseTaker();
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
                runLeaseRenewer();
            } catch (LeasingException e) {
                log.error("LeasingException encountered in lease renewing thread", e);
            } catch (Throwable t) {
                log.error("Throwable encountered in lease renewing thread", t);
            }
        }

    }

    @Override
    public void initialize() throws ProvisionedThroughputException, DependencyException, IllegalStateException {
        final boolean newTableCreated =
                leaseRefresher.createLeaseTableIfNotExists();
        if (newTableCreated) {
            log.info("Created new lease table for coordinator with pay per request billing mode.");
        }
        // Need to wait for table in active state.
        final long secondsBetweenPolls = 10L;
        final long timeoutSeconds = 600L;
        final boolean isTableActive = leaseRefresher.waitUntilLeaseTableExists(secondsBetweenPolls, timeoutSeconds);
        if (!isTableActive) {
            throw new DependencyException(new IllegalStateException("Creating table timeout"));
        }
    }

    @Override
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

    @Override
    public void runLeaseTaker() throws DependencyException, InvalidStateException {
        MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, "TakeLeases");
        long startTime = System.currentTimeMillis();
        boolean success = false;

        try {
            Map<String, Lease> takenLeases = leaseTaker.takeLeases();

            // Only add taken leases to renewer if coordinator is still running.
            synchronized (shutdownLock) {
                if (running) {
                    leaseRenewer.addLeasesToRenew(takenLeases.values());
                }
            }

            success = true;
        } finally {
            MetricsUtil.addWorkerIdentifier(scope, workerIdentifier());
            MetricsUtil.addSuccessAndLatency(scope, success, startTime, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(scope);
        }
    }

    @Override
    public void runLeaseRenewer() throws DependencyException, InvalidStateException {
        leaseRenewer.renewLeases();
    }

    @Override
    public Collection<Lease> getAssignments() {
        return leaseRenewer.getCurrentlyHeldLeases().values();
    }

    @Override
    public List<Lease> allLeases() {
        return leaseTaker.allLeases();
    }

    @Override
    public Lease getCurrentlyHeldLease(String leaseKey) {
        return leaseRenewer.getCurrentlyHeldLease(leaseKey);
    }

    @Override
    public String workerIdentifier() {
        return leaseTaker.getWorkerIdentifier();
    }

    @Override
    public LeaseRefresher leaseRefresher() {
        return leaseRefresher;
    }

    @Override
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

    @Override
    public void stopLeaseTaker() {
        if (takerFuture != null) {
            takerFuture.cancel(false);
        }
    }

    @Override
    public void dropLease(final Lease lease) {
        synchronized (shutdownLock) {
            if (lease != null) {
                leaseRenewer.dropLease(lease);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean updateLease(final Lease lease, final UUID concurrencyToken, final String operation,
            final String singleStreamShardId) throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return leaseRenewer.updateLease(lease, concurrencyToken, operation, singleStreamShardId);
    }

    /**
     * Returns executor service that should be used for lease renewal.
     * @param maximumPoolSize Maximum allowed thread pool size
     * @return Executor service that should be used for lease renewal.
     */
    private static ExecutorService getLeaseRenewalExecutorService(int maximumPoolSize) {
        int coreLeaseCount = Math.max(maximumPoolSize / 4, 2);

        return new ThreadPoolExecutor(coreLeaseCount, maximumPoolSize, 60, TimeUnit.SECONDS,
                new LinkedTransferQueue<>(), LEASE_RENEWAL_THREAD_FACTORY);
    }

    /**
     * {@inheritDoc}
     *
     * <p>NOTE: This method is deprecated. Please set the initial capacity through the constructor.</p>
     */
    @Override
    @Deprecated
    public DynamoDBLeaseCoordinator initialLeaseTableReadCapacity(long readCapacity) {
        if (readCapacity <= 0) {
            throw new IllegalArgumentException("readCapacity should be >= 1");
        }
        initialLeaseTableReadCapacity = readCapacity;
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>NOTE: This method is deprecated. Please set the initial capacity through the constructor.</p>
     */
    @Override
    @Deprecated
    public DynamoDBLeaseCoordinator initialLeaseTableWriteCapacity(long writeCapacity) {
        if (writeCapacity <= 0) {
            throw new IllegalArgumentException("writeCapacity should be >= 1");
        }
        initialLeaseTableWriteCapacity = writeCapacity;
        return this;
    }
}
