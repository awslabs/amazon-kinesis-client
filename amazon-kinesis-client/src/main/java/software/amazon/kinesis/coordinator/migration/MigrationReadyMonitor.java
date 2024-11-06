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
package software.amazon.kinesis.coordinator.migration;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;

import static software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl.METRICS_OPERATION;

/**
 * Monitor for KCL workers 3.x readiness. This monitor is started on all workers but only
 * executed on the leader of the fleet. The leader determines 3.x readiness if GSI of the lease
 * table is active and all lease owners are emitting WorkerMetricStats. The monitor performs this
 * check periodically and will invoke callback if the readiness conditions are true. Monitor
 * needs to be explicitly cancelled after the readiness trigger has successfully been handled.
 *
 * Thread safety - Guard for safety against public method invocation and internal runnable method.
 */
@Slf4j
@ThreadSafe
@KinesisClientInternalApi
public class MigrationReadyMonitor implements Runnable {
    private static final long MONITOR_INTERVAL_MILLIS = Duration.ofMinutes(1).toMillis();
    private static final long LOG_INTERVAL_NANOS = Duration.ofMinutes(5).toNanos();

    /**
     * Default retry attempt for loading leases and workers before giving up.
     */
    private static final int DDB_LOAD_RETRY_ATTEMPT = 1;

    private final MetricsFactory metricsFactory;
    private final Callable<Long> timeProvider;
    private final LeaderDecider leaderDecider;
    private final String currentWorkerId;
    private final WorkerMetricStatsDAO workerMetricStatsDAO;
    private final long workerMetricStatsExpirySeconds;
    private final LeaseRefresher leaseRefresher;
    private final ScheduledExecutorService stateMachineThreadPool;
    private final MonitorTriggerStabilizer triggerStabilizer;

    private final LogRateLimiter rateLimitedStatusLogger = new LogRateLimiter(LOG_INTERVAL_NANOS);
    private ScheduledFuture<?> scheduledFuture;
    private boolean gsiStatusReady;
    private boolean workerMetricsReady;
    private Set<String> lastKnownUniqueLeaseOwners = new HashSet<>();
    private Set<String> lastKnownWorkersWithActiveWorkerMetrics = new HashSet<>();

    public MigrationReadyMonitor(
            final MetricsFactory metricsFactory,
            final Callable<Long> timeProvider,
            final LeaderDecider leaderDecider,
            final String currentWorkerId,
            final WorkerMetricStatsDAO workerMetricStatsDAO,
            final long workerMetricsExpirySeconds,
            final LeaseRefresher leaseRefresher,
            final ScheduledExecutorService stateMachineThreadPool,
            final Runnable callback,
            final long callbackStabilizationInSeconds) {
        this.metricsFactory = metricsFactory;
        this.timeProvider = timeProvider;
        this.leaderDecider = leaderDecider;
        this.currentWorkerId = currentWorkerId;
        this.workerMetricStatsDAO = workerMetricStatsDAO;
        this.workerMetricStatsExpirySeconds = workerMetricsExpirySeconds;
        this.leaseRefresher = leaseRefresher;
        this.stateMachineThreadPool = stateMachineThreadPool;
        this.triggerStabilizer =
                new MonitorTriggerStabilizer(timeProvider, callbackStabilizationInSeconds, callback, currentWorkerId);
    }

    public synchronized void startMonitor() {
        if (Objects.isNull(scheduledFuture)) {

            log.info("Starting migration ready monitor");
            scheduledFuture = stateMachineThreadPool.scheduleWithFixedDelay(
                    this, MONITOR_INTERVAL_MILLIS, MONITOR_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        } else {
            log.info("Ignoring monitor request, since it is already started");
        }
    }

    /**
     * Cancel the monitor. Once the method returns callback will not be invoked,
     *  but callback can be invoked reentrantly before this method returns.
     */
    public synchronized void cancel() {
        if (Objects.nonNull(scheduledFuture)) {
            log.info("Cancelled migration ready monitor");
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        } else {
            log.info("{} is currently not active", this);
        }
    }

    @Override
    public synchronized void run() {
        try {
            if (Thread.currentThread().isInterrupted()) {
                log.info("{} cancelled, exiting...", this);
                return;
            }
            if (!leaderDecider.isLeader(currentWorkerId)) {
                log.debug("Not the leader, not performing migration ready check {}", this);
                triggerStabilizer.reset();
                lastKnownUniqueLeaseOwners.clear();
                lastKnownWorkersWithActiveWorkerMetrics.clear();
                return;
            }

            triggerStabilizer.call(isReadyForUpgradeTo3x());
            rateLimitedStatusLogger.log(() -> log.info("Monitor ran successfully {}", this));
        } catch (final Throwable t) {
            log.warn("{} failed, will retry after {}", this, MONITOR_INTERVAL_MILLIS, t);
        }
    }

    @Override
    public String toString() {
        return new StringBuilder("UpgradeReadyMonitor[")
                .append("G=")
                .append(gsiStatusReady)
                .append(",W=")
                .append(workerMetricsReady)
                .append("]")
                .toString();
    }

    private boolean isReadyForUpgradeTo3x() throws DependencyException {
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, METRICS_OPERATION);
        try {
            // If GSI is not ready, optimize to not check if worker metrics are being emitted
            final boolean localGsiReadyStatus = leaseRefresher.isLeaseOwnerToLeaseKeyIndexActive();
            if (localGsiReadyStatus != gsiStatusReady) {
                gsiStatusReady = localGsiReadyStatus;
                log.info("Gsi ready status changed to {}", gsiStatusReady);
            } else {
                log.debug("GsiReady status {}", gsiStatusReady);
            }
            return gsiStatusReady && areLeaseOwnersEmittingWorkerMetrics();
        } finally {
            scope.addData("GsiReadyStatus", gsiStatusReady ? 1 : 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData(
                    "WorkerMetricsReadyStatus", workerMetricsReady ? 1 : 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(scope);
        }
    }

    private boolean areLeaseOwnersEmittingWorkerMetrics() {
        final CompletableFuture<List<Lease>> leaseListFuture = loadLeaseListAsync();
        final CompletableFuture<List<WorkerMetricStats>> workerMetricsFuture = loadWorkerMetricStats();

        final List<Lease> leaseList = leaseListFuture.join();
        final Set<String> leaseOwners = getUniqueLeaseOwnersFromLeaseTable(leaseList);
        final List<WorkerMetricStats> workerMetricStatsList = workerMetricsFuture.join();
        final Set<String> workersWithActiveWorkerMetrics = getWorkersWithActiveWorkerMetricStats(workerMetricStatsList);

        // Leases are not checked for expired condition because:
        // If some worker has gone down and is not active, but has lease assigned to it, those leases
        // maybe expired. Since the worker is down, it may not have worker-metrics, or worker-metrics may not be active,
        // In that case, the migration condition is not considered to be met.
        // However, those leases should be assigned to another worker and so the check in the next
        // iteration could succeed. This is intentional to make sure all leases owners are accounted for
        // and the old owner does not come back up without worker metrics and reacquires the lease.
        final boolean localWorkerMetricsReady = leaseOwners.equals(workersWithActiveWorkerMetrics);
        if (localWorkerMetricsReady != workerMetricsReady) {
            workerMetricsReady = localWorkerMetricsReady;
            log.info("WorkerMetricStats status changed to {}", workerMetricsReady);
            log.info("Lease List {}", leaseList);
            log.info("WorkerMetricStats {}", workerMetricStatsList);
        } else {
            log.debug("WorkerMetricStats ready status {}", workerMetricsReady);
        }

        if (lastKnownUniqueLeaseOwners == null) {
            log.info("Unique lease owners {}", leaseOwners);
        } else if (!lastKnownUniqueLeaseOwners.equals(leaseOwners)) {
            log.info("Unique lease owners changed to {}", leaseOwners);
        }
        lastKnownUniqueLeaseOwners = leaseOwners;

        if (lastKnownWorkersWithActiveWorkerMetrics == null) {
            log.info("Workers with active worker metric stats {}", workersWithActiveWorkerMetrics);
        } else if (!lastKnownWorkersWithActiveWorkerMetrics.equals(workersWithActiveWorkerMetrics)) {
            log.info("Workers with active worker metric stats changed {}", workersWithActiveWorkerMetrics);
        }
        lastKnownWorkersWithActiveWorkerMetrics = workersWithActiveWorkerMetrics;

        return workerMetricsReady;
    }

    private Set<String> getUniqueLeaseOwnersFromLeaseTable(final List<Lease> leaseList) {
        return leaseList.stream().map(Lease::leaseOwner).collect(Collectors.toSet());
    }

    private Set<String> getWorkersWithActiveWorkerMetricStats(final List<WorkerMetricStats> workerMetricStats) {
        final long nowInSeconds = Duration.ofMillis(now(timeProvider)).getSeconds();
        return workerMetricStats.stream()
                .filter(metricStats -> isWorkerMetricStatsActive(metricStats, nowInSeconds))
                .map(WorkerMetricStats::getWorkerId)
                .collect(Collectors.toSet());
    }

    private boolean isWorkerMetricStatsActive(final WorkerMetricStats metricStats, final long nowInSeconds) {
        return (metricStats.getLastUpdateTime() + workerMetricStatsExpirySeconds) > nowInSeconds;
    }

    private CompletableFuture<List<WorkerMetricStats>> loadWorkerMetricStats() {
        return CompletableFuture.supplyAsync(() -> loadWithRetry(workerMetricStatsDAO::getAllWorkerMetricStats));
    }

    private CompletableFuture<List<Lease>> loadLeaseListAsync() {
        return CompletableFuture.supplyAsync(() -> loadWithRetry(leaseRefresher::listLeases));
    }

    private <T> T loadWithRetry(final Callable<T> loadFunction) {
        int retryAttempt = 0;
        while (true) {
            try {
                return loadFunction.call();
            } catch (final Exception e) {
                if (retryAttempt < DDB_LOAD_RETRY_ATTEMPT) {
                    log.warn(
                            "Failed to load : {}, retrying",
                            loadFunction.getClass().getName(),
                            e);
                    retryAttempt++;
                } else {
                    throw new CompletionException(e);
                }
            }
        }
    }

    private static long now(final Callable<Long> timeProvider) {
        try {
            return timeProvider.call();
        } catch (final Exception e) {
            log.debug("Time provider threw exception, using System.currentTimeMillis", e);
            return System.currentTimeMillis();
        }
    }

    /**
     * Stabilize the monitor trigger before invoking the callback
     * to ensure we are consistently seeing the trigger for a configured
     * stabilizationDurationInMillis
     */
    private static class MonitorTriggerStabilizer {
        private final Callable<Long> timeProvider;
        private final long stabilizationDurationInSeconds;
        private final Runnable callback;
        private final String currentWorkerId;
        private final LogRateLimiter rateLimitedTriggerStatusLogger;

        private long lastToggleTimeInMillis;
        private boolean currentTriggerStatus;

        public MonitorTriggerStabilizer(
                final Callable<Long> timeProvider,
                final long stabilizationDurationInSeconds,
                final Runnable callback,
                final String currentWorkerId) {
            this.timeProvider = timeProvider;
            this.stabilizationDurationInSeconds = stabilizationDurationInSeconds;
            this.callback = callback;
            this.currentWorkerId = currentWorkerId;
            this.rateLimitedTriggerStatusLogger = new LogRateLimiter(LOG_INTERVAL_NANOS);
        }

        public void call(final boolean isMonitorTriggered) {
            final long now = now(timeProvider);
            if (currentTriggerStatus != isMonitorTriggered) {
                log.info("Trigger status has changed to {}", isMonitorTriggered);
                currentTriggerStatus = isMonitorTriggered;
                lastToggleTimeInMillis = now;
            }

            if (currentTriggerStatus) {
                final long deltaSeconds =
                        Duration.ofMillis(now - lastToggleTimeInMillis).getSeconds();
                if (deltaSeconds >= stabilizationDurationInSeconds) {
                    log.info("Trigger has been consistently true for {}s, invoking callback", deltaSeconds);
                    callback.run();
                } else {
                    rateLimitedTriggerStatusLogger.log(() -> log.info(
                            "Trigger has been true for {}s, waiting for stabilization time of {}s",
                            deltaSeconds,
                            stabilizationDurationInSeconds));
                }
            }
        }

        public void reset() {
            if (currentTriggerStatus) {
                log.info("This worker {} is no longer the leader, reset current status", currentWorkerId);
            }
            currentTriggerStatus = false;
        }
    }

    @RequiredArgsConstructor
    private static class LogRateLimiter {
        private final long logIntervalInNanos;

        private long nextLogTime = System.nanoTime();

        public void log(final Runnable logger) {
            final long now = System.nanoTime();
            if (now >= nextLogTime) {
                logger.run();
                nextLogTime = now + logIntervalInNanos;
            }
        }
    }
}
