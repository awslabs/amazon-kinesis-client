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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseDiscoverer;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseRenewer;
import software.amazon.kinesis.leases.LeaseStatsRecorder;
import software.amazon.kinesis.leases.LeaseTaker;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.LeaseGracefulShutdownHandler;
import software.amazon.kinesis.lifecycle.ShardConsumer;
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
            .setNameFormat("LeaseCoordinator-%04d")
            .setDaemon(true)
            .build();
    private static final ThreadFactory LEASE_RENEWAL_THREAD_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat("LeaseRenewer-%04d")
            .setDaemon(true)
            .build();
    private static final ThreadFactory LEASE_DISCOVERY_THREAD_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat("LeaseDiscovery-%04d")
            .setDaemon(true)
            .build();

    private final LeaseRenewer leaseRenewer;
    private final LeaseTaker leaseTaker;
    private final LeaseDiscoverer leaseDiscoverer;
    private final long renewerIntervalMillis;
    private final long takerIntervalMillis;
    private final long leaseDiscovererIntervalMillis;
    private final ExecutorService leaseRenewalThreadpool;
    private final ExecutorService leaseDiscoveryThreadPool;
    private final LeaseRefresher leaseRefresher;
    private final LeaseStatsRecorder leaseStatsRecorder;
    private final LeaseGracefulShutdownHandler leaseGracefulShutdownHandler;
    private long initialLeaseTableReadCapacity;
    private long initialLeaseTableWriteCapacity;
    protected final MetricsFactory metricsFactory;

    private final Object shutdownLock = new Object();
    private final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig;
    private ScheduledExecutorService leaseCoordinatorThreadPool;
    private ScheduledFuture<?> leaseDiscoveryFuture;
    private ScheduledFuture<?> takerFuture;

    private volatile boolean running = false;

    /**
     * Constructor.
     *
     * @param leaseRefresher
     *            LeaseRefresher instance to use
     * @param workerIdentifier
     *            Identifies the worker (e.g. useful to track lease ownership)
     * @param leaseDurationMillis
     *            Duration of a lease
     * @param enablePriorityLeaseAssignment
     *            Whether to enable priority lease assignment for very expired leases
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
    public DynamoDBLeaseCoordinator(
            final LeaseRefresher leaseRefresher,
            final String workerIdentifier,
            final long leaseDurationMillis,
            final boolean enablePriorityLeaseAssignment,
            final long epsilonMillis,
            final int maxLeasesForWorker,
            final int maxLeasesToStealAtOneTime,
            final int maxLeaseRenewerThreadCount,
            final long initialLeaseTableReadCapacity,
            final long initialLeaseTableWriteCapacity,
            final MetricsFactory metricsFactory,
            final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig,
            final LeaseManagementConfig.GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig,
            final ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap) {
        this.leaseRefresher = leaseRefresher;
        this.leaseRenewalThreadpool = createExecutorService(maxLeaseRenewerThreadCount, LEASE_RENEWAL_THREAD_FACTORY);
        this.leaseTaker = new DynamoDBLeaseTaker(leaseRefresher, workerIdentifier, leaseDurationMillis, metricsFactory)
                .withMaxLeasesForWorker(maxLeasesForWorker)
                .withMaxLeasesToStealAtOneTime(maxLeasesToStealAtOneTime)
                .withEnablePriorityLeaseAssignment(enablePriorityLeaseAssignment);
        this.renewerIntervalMillis = getRenewerTakerIntervalMillis(leaseDurationMillis, epsilonMillis);
        this.takerIntervalMillis = (leaseDurationMillis + epsilonMillis) * 2;
        // Should run once every leaseDurationMillis to identify new leases before expiry.
        this.leaseDiscovererIntervalMillis = leaseDurationMillis - epsilonMillis;
        this.leaseStatsRecorder = new LeaseStatsRecorder(renewerIntervalMillis, System::currentTimeMillis);
        this.leaseGracefulShutdownHandler = LeaseGracefulShutdownHandler.create(
                gracefulLeaseHandoffConfig.gracefulLeaseHandoffTimeoutMillis(), shardInfoShardConsumerMap, this);
        this.leaseRenewer = new DynamoDBLeaseRenewer(
                leaseRefresher,
                workerIdentifier,
                leaseDurationMillis,
                leaseRenewalThreadpool,
                metricsFactory,
                leaseStatsRecorder,
                leaseGracefulShutdownHandler::enqueueShutdown);
        this.leaseDiscoveryThreadPool =
                createExecutorService(maxLeaseRenewerThreadCount, LEASE_DISCOVERY_THREAD_FACTORY);
        this.leaseDiscoverer = new DynamoDBLeaseDiscoverer(
                this.leaseRefresher, this.leaseRenewer, metricsFactory, workerIdentifier, leaseDiscoveryThreadPool);
        if (initialLeaseTableReadCapacity <= 0) {
            throw new IllegalArgumentException("readCapacity should be >= 1");
        }
        this.initialLeaseTableReadCapacity = initialLeaseTableReadCapacity;
        if (initialLeaseTableWriteCapacity <= 0) {
            throw new IllegalArgumentException("writeCapacity should be >= 1");
        }
        this.initialLeaseTableWriteCapacity = initialLeaseTableWriteCapacity;
        this.metricsFactory = metricsFactory;
        this.workerUtilizationAwareAssignmentConfig = workerUtilizationAwareAssignmentConfig;

        log.info(
                "With failover time {} ms and epsilon {} ms, LeaseCoordinator will renew leases every {} ms, take"
                        + "leases every {} ms, process maximum of {} leases and steal {} lease(s) at a time.",
                leaseDurationMillis,
                epsilonMillis,
                renewerIntervalMillis,
                takerIntervalMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime);
    }

    @RequiredArgsConstructor
    private class LeaseDiscoveryRunnable implements Runnable {
        private final MigrationAdaptiveLeaseAssignmentModeProvider leaseAssignmentModeProvider;

        @Override
        public void run() {
            try {
                // LeaseDiscoverer is run in WORKER_UTILIZATION_AWARE_ASSIGNMENT mode only
                synchronized (shutdownLock) {
                    if (!leaseAssignmentModeProvider
                            .getLeaseAssignmentMode()
                            .equals(
                                    MigrationAdaptiveLeaseAssignmentModeProvider.LeaseAssignmentMode
                                            .WORKER_UTILIZATION_AWARE_ASSIGNMENT)) {
                        return;
                    }
                    if (running) {
                        leaseRenewer.addLeasesToRenew(leaseDiscoverer.discoverNewLeases());
                    }
                }
            } catch (Exception e) {
                log.error("Failed to execute lease discovery", e);
            }
        }
    }

    @RequiredArgsConstructor
    private class TakerRunnable implements Runnable {
        private final MigrationAdaptiveLeaseAssignmentModeProvider leaseAssignmentModeProvider;

        @Override
        public void run() {
            try {
                // LeaseTaker is run in DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT mode only
                synchronized (shutdownLock) {
                    if (!leaseAssignmentModeProvider
                            .getLeaseAssignmentMode()
                            .equals(
                                    MigrationAdaptiveLeaseAssignmentModeProvider.LeaseAssignmentMode
                                            .DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT)) {
                        return;
                    }
                }
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
        final boolean newTableCreated = leaseRefresher.createLeaseTableIfNotExists();
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
    public void start(final MigrationAdaptiveLeaseAssignmentModeProvider leaseAssignmentModeProvider)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        leaseRenewer.initialize();
        // At max, we need 3 threads - lease renewer, lease taker, lease discoverer - to run without contention.
        leaseCoordinatorThreadPool = Executors.newScheduledThreadPool(3, LEASE_COORDINATOR_THREAD_FACTORY);

        // During migration to KCLv3.x from KCLv2.x, lease assignment mode can change dynamically, so
        // both lease assignment algorithms will be started but only one will execute based on
        // leaseAssignmentModeProvider.getLeaseAssignmentMode(). However for new applications starting in
        // KCLv3.x or applications successfully migrated to KCLv3.x, lease assignment mode will not
        // change dynamically and will always be WORKER_UTILIZATION_AWARE_ASSIGNMENT, therefore
        // don't initialize KCLv2.x lease assignment algorithm components that are not needed.
        if (leaseAssignmentModeProvider.dynamicModeChangeSupportNeeded()) {
            // Taker runs with fixed DELAY because we want it to run slower in the event of performance degradation.
            takerFuture = leaseCoordinatorThreadPool.scheduleWithFixedDelay(
                    new TakerRunnable(leaseAssignmentModeProvider), 0L, takerIntervalMillis, TimeUnit.MILLISECONDS);
        }

        leaseDiscoveryFuture = leaseCoordinatorThreadPool.scheduleAtFixedRate(
                new LeaseDiscoveryRunnable(leaseAssignmentModeProvider),
                0L,
                leaseDiscovererIntervalMillis,
                TimeUnit.MILLISECONDS);

        // Renewer runs at fixed INTERVAL because we want it to run at the same rate in the event of degradation.
        leaseCoordinatorThreadPool.scheduleAtFixedRate(
                new RenewerRunnable(), 0L, renewerIntervalMillis, TimeUnit.MILLISECONDS);

        leaseGracefulShutdownHandler.start();
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
                    log.info(
                            "Worker {} has successfully stopped lease-tracking threads",
                            leaseTaker.getWorkerIdentifier());
                } else {
                    leaseCoordinatorThreadPool.shutdownNow();
                    log.info(
                            "Worker {} stopped lease-tracking threads {} ms after stop",
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
        leaseCoordinatorThreadPool.shutdownNow();
        leaseGracefulShutdownHandler.stop();
        synchronized (shutdownLock) {
            leaseRenewer.clearCurrentlyHeldLeases();
            running = false;
        }
    }

    @Override
    public void stopLeaseTaker() {
        if (takerFuture != null) {
            takerFuture.cancel(false);
            leaseDiscoveryFuture.cancel(false);
            // the method is called in worker graceful shutdown. We want to stop any further lease shutdown
            // so we don't interrupt worker shutdown.
            leaseGracefulShutdownHandler.stop();
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
    public boolean updateLease(
            final Lease lease, final UUID concurrencyToken, final String operation, final String singleStreamShardId)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return leaseRenewer.updateLease(lease, concurrencyToken, operation, singleStreamShardId);
    }

    /**
     * Returns executor service for given ThreadFactory.
     * @param maximumPoolSize Maximum allowed thread pool size
     * @return Executor service
     */
    private static ExecutorService createExecutorService(final int maximumPoolSize, final ThreadFactory threadFactory) {
        int coreLeaseCount = Math.max(maximumPoolSize / 4, 2);

        return new ThreadPoolExecutor(
                coreLeaseCount, maximumPoolSize, 60, TimeUnit.SECONDS, new LinkedTransferQueue<>(), threadFactory);
    }

    @Override
    public List<ShardInfo> getCurrentAssignments() {
        Collection<Lease> leases = getAssignments();
        return convertLeasesToAssignments(leases);
    }

    private static List<ShardInfo> convertLeasesToAssignments(final Collection<Lease> leases) {
        if (leases == null) {
            return Collections.emptyList();
        }
        return leases.stream()
                .map(DynamoDBLeaseCoordinator::convertLeaseToAssignment)
                .collect(Collectors.toList());
    }

    /**
     * Utility method to convert the basic lease or multistream lease to ShardInfo
     * @param lease
     * @return ShardInfo
     */
    public static ShardInfo convertLeaseToAssignment(final Lease lease) {
        if (lease instanceof MultiStreamLease) {
            return new ShardInfo(
                    ((MultiStreamLease) lease).shardId(),
                    lease.concurrencyToken().toString(),
                    lease.parentShardIds(),
                    lease.checkpoint(),
                    ((MultiStreamLease) lease).streamIdentifier());
        } else {
            return new ShardInfo(
                    lease.leaseKey(), lease.concurrencyToken().toString(), lease.parentShardIds(), lease.checkpoint());
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>NOTE: This method is deprecated. Please set the initial capacity through the constructor.</p>
     *
     * This is a method of the public lease coordinator interface.
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
     *
     * This is a method of the public lease coordinator interface.
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

    @Override
    public LeaseStatsRecorder leaseStatsRecorder() {
        return leaseStatsRecorder;
    }
}
