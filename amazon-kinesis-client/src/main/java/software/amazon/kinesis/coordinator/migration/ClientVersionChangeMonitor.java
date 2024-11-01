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
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static software.amazon.kinesis.coordinator.migration.MigrationState.MIGRATION_HASH_KEY;
import static software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl.METRICS_OPERATION;

/**
 * Change monitor for MigrationState.clientVersion to notify a callback if the value
 * changes from a given value. This monitor will be run to monitor
 * rollback, roll-forward and also upgrade to 3.x scenarios. Look at {@link ClientVersion}
 * for more details.
 *
 * Since all KCL workers will be running the monitor, the monitor poll interval uses
 * a random jitter to stagger the reads to ddb.
 *
 * The class is thread-safe and will invoke callback on a separate thread.
 */
@Slf4j
@RequiredArgsConstructor
@ThreadSafe
public class ClientVersionChangeMonitor implements Runnable {

    /**
     * Interface of a callback to invoke when monitor condition is true.
     */
    public interface ClientVersionChangeCallback {
        void accept(final MigrationState currentMigrationState) throws InvalidStateException, DependencyException;
    }

    private static final long MONITOR_INTERVAL_MILLIS = Duration.ofMinutes(1).toMillis();
    private static final double JITTER_FACTOR = 0.1;

    private final MetricsFactory metricsFactory;
    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ScheduledExecutorService stateMachineThreadPool;
    private final ClientVersionChangeCallback callback;
    private final ClientVersion expectedVersion;
    private final Random random;
    private long monitorIntervalMillis;

    private ScheduledFuture<?> scheduledFuture;

    public synchronized void startMonitor() {
        if (scheduledFuture == null) {
            final long jitter = (long) (random.nextDouble() * MONITOR_INTERVAL_MILLIS * JITTER_FACTOR);
            monitorIntervalMillis = MONITOR_INTERVAL_MILLIS + jitter;
            log.info(
                    "Monitoring for MigrationState client version change from {} every {}ms",
                    expectedVersion,
                    monitorIntervalMillis);
            scheduledFuture = stateMachineThreadPool.scheduleWithFixedDelay(
                    this, monitorIntervalMillis, monitorIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public String toString() {
        return new StringBuilder(getClass().getSimpleName())
                .append("[")
                .append(expectedVersion)
                .append("]")
                .toString();
    }

    /**
     * Cancel the monitor explicity before the condition is met, e.g. when the worker is going down.
     * Note on synchronization: callback of this monitor is invoked while holding the lock on this monitor object.
     * If cancel is called from within the same lock context that callback uses, then it can lead to
     * deadlock. Ensure synchronization context between callback the caller of cancel is not shared.
     */
    public synchronized void cancel() {
        if (scheduledFuture != null) {
            log.info("Cancelling {}", this);
            scheduledFuture.cancel(false);
        } else {
            log.info("Monitor {} is not running", this);
        }
    }

    @Override
    public synchronized void run() {
        try {
            if (scheduledFuture == null) {
                log.debug("Monitor has been cancelled, not running...");
                return;
            }

            final MigrationState migrationState =
                    (MigrationState) coordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY);
            if (migrationState != null) {
                if (migrationState.getClientVersion() != expectedVersion) {
                    log.info("MigrationState client version has changed {}, invoking monitor callback", migrationState);
                    callback.accept(migrationState);
                    log.info("Callback successful, monitoring cancelling itself.");
                    // stop further monitoring
                    scheduledFuture.cancel(false);
                    scheduledFuture = null;
                } else {
                    emitMetrics();
                    log.debug("No change detected {}", this);
                }
            }
        } catch (final Exception e) {
            log.warn(
                    "Exception occurred when monitoring for client version change from {}, will retry in {}",
                    expectedVersion,
                    monitorIntervalMillis,
                    e);
        }
    }

    private void emitMetrics() {
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, METRICS_OPERATION);
        try {
            switch (expectedVersion) {
                case CLIENT_VERSION_3X_WITH_ROLLBACK:
                    scope.addData("CurrentState:3xWorker", 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
                    break;
                case CLIENT_VERSION_2X:
                case CLIENT_VERSION_UPGRADE_FROM_2X:
                    scope.addData("CurrentState:2xCompatibleWorker", 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
                    break;
                default:
                    throw new IllegalStateException(String.format("Unexpected version %s", expectedVersion.name()));
            }
        } finally {
            MetricsUtil.endScope(scope);
        }
    }
}
