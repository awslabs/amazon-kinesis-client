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
package software.amazon.kinesis.lifecycle;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;

@Slf4j
@Accessors(fluent = true)
@KinesisClientInternalApi
class ShardConsumerSubscriber implements Subscriber<RecordsRetrieved> {
    // Same operation as ProcessTask's shard-level metrics: a failed dispatch is a ProcessTask attempt that failed
    // before the handoff.
    private static final String METRICS_OPERATION = "ProcessTask";
    // App-level operation (no shard dimension), shared with ProcessTask, so customers can alarm across all shards.
    private static final String APPLICATION_TRACKER_OPERATION = "ApplicationTracker";
    private static final String DISPATCH_FAILURE_METRIC = "DispatchFailure";
    private static final String DISPATCH_FATAL_ERROR_METRIC = "DispatchFatalError";

    private final RecordsPublisher recordsPublisher;
    private final Scheduler scheduler;
    private final int bufferSize;
    private final ShardConsumer shardConsumer;
    private final int readTimeoutsToIgnoreBeforeWarning;
    private final String shardInfoId;
    private final MetricsFactory metricsFactory;
    private volatile int readTimeoutSinceLastRead = 0;

    @VisibleForTesting
    final Object lockObject = new Object();
    // This holds the last time an attempt of request to upstream service was made including the first try to
    // establish subscription.
    private Instant lastRequestTime = null;
    private RecordsRetrieved lastAccepted = null;

    private Subscription subscription;

    @Getter
    private volatile Instant lastDataArrival;

    /**
     * Fatal failure (an {@link Error}, or anything unexpected) raised while dispatching a batch. Once set, this shard
     * stops consuming.
     */
    private volatile Throwable dispatchFailure;

    /**
     * Set once this subscriber gives up on dispatching (shutdown requested, interrupted, or fatal error). Once set,
     * no further records are requested, delivered, or resubscribed for.
     */
    private volatile boolean dispatchStopped = false;

    @VisibleForTesting
    long dispatchRetryInitialBackoffMillis = 500L;

    @VisibleForTesting
    long dispatchRetryMaxBackoffMillis = 10_000L;

    @Getter(AccessLevel.PACKAGE)
    private volatile Throwable retrievalFailure;

    ShardConsumerSubscriber(
            RecordsPublisher recordsPublisher,
            ExecutorService executorService,
            int bufferSize,
            ShardConsumer shardConsumer,
            int readTimeoutsToIgnoreBeforeWarning,
            MetricsFactory metricsFactory) {
        this.recordsPublisher = recordsPublisher;
        this.scheduler = Schedulers.from(executorService);
        this.bufferSize = bufferSize;
        this.shardConsumer = shardConsumer;
        this.readTimeoutsToIgnoreBeforeWarning = readTimeoutsToIgnoreBeforeWarning;
        this.shardInfoId = ShardInfo.getLeaseKey(shardConsumer.shardInfo());
        this.metricsFactory = metricsFactory;
    }

    void startSubscriptions() {
        synchronized (lockObject) {
            if (dispatchStopped) {
                // Stopped for good (shutdown or fatal error); never resubscribe and pull more records.
                return;
            }
            // Setting the lastRequestTime to allow for health checks to restart subscriptions if they failed to
            // during initial try.
            lastRequestTime = Instant.now();
            if (lastAccepted != null) {
                recordsPublisher.restartFrom(lastAccepted);
            }
            Flowable<RecordsRetrieved> flowable =
                    Flowable.fromPublisher(recordsPublisher).subscribeOn(scheduler);

            // When buffer size is set, RxJava applies buffering to control backpressure.
            // With buffer: non-blocking - publisher continues while subscriber processes asynchronously.
            // Without buffer: blocking - publisher waits for subscriber to finish each record.
            if (bufferSize != 0) {
                flowable = flowable.observeOn(scheduler, true, bufferSize);
            }

            flowable.subscribe(new ShardConsumerNotifyingSubscriber(this, recordsPublisher));
        }
    }

    Throwable healthCheck(long maxTimeBetweenRequests) {
        Throwable result = restartIfFailed();
        if (result == null) {
            restartIfRequestTimerExpired(maxTimeBetweenRequests);
        }
        return result;
    }

    Throwable getDispatchFailure() {
        synchronized (lockObject) {
            return dispatchFailure;
        }
    }

    private Throwable restartIfFailed() {
        Throwable oldFailure = null;
        if (retrievalFailure != null) {
            synchronized (lockObject) {
                String logMessage =
                        String.format("%s: Failure occurred in retrieval.  Restarting data requests", shardInfoId);
                if (retrievalFailure instanceof RetryableRetrievalException) {
                    log.debug(logMessage, retrievalFailure.getCause());
                } else {
                    log.warn(logMessage, retrievalFailure);
                }
                oldFailure = retrievalFailure;
                retrievalFailure = null;
            }
            startSubscriptions();
        }

        return oldFailure;
    }

    private void restartIfRequestTimerExpired(long maxTimeBetweenRequests) {
        synchronized (lockObject) {
            if (lastRequestTime != null) {
                Instant now = Instant.now();
                Duration timeSinceLastResponse = Duration.between(lastRequestTime, now);
                if (timeSinceLastResponse.toMillis() > maxTimeBetweenRequests) {
                    log.error(
                            // CHECKSTYLE.OFF: LineLength
                            "{}: Last request was dispatched at {}, but no response as of {} ({}).  Cancelling subscription, and restarting. Last successful request details -- {}",
                            // CHECKSTYLE.ON: LineLength
                            shardInfoId,
                            lastRequestTime,
                            now,
                            timeSinceLastResponse,
                            recordsPublisher.getLastSuccessfulRequestDetails());
                    cancel();

                    // Start the subscription again which will update the lastRequestTime as well.
                    startSubscriptions();
                }
            }
        }
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscription = s;
        subscription.request(1);
    }

    @Override
    public void onNext(RecordsRetrieved input) {
        if (dispatchStopped) {
            // Already stopped; drop anything still in flight from upstream.
            return;
        }
        synchronized (lockObject) {
            lastRequestTime = null;
        }
        lastDataArrival = Instant.now();

        boolean handedOff = false;
        try {
            handedOff = dispatchWithRetry(input);
        } catch (Throwable t) {
            // Unexpected escape. onNext must not throw (Reactive Streams 2.13), so record it instead (keeping any fatal
            // error already recorded) and let healthCheck surface the halt.
            log.error(
                    "{}: Unexpected failure while dispatching batch, stopping consumption of this shard",
                    shardInfoId,
                    t);
            synchronized (lockObject) {
                if (dispatchFailure == null) {
                    dispatchFailure = t;
                }
            }
        } finally {
            // Any exit without handing the batch off (fatal error, shutdown, interrupt, or anything unexpected) stops
            // this subscriber, so the read position never advances past an undelivered batch.
            if (!handedOff) {
                stopDispatching();
            }
        }
        if (!handedOff) {
            return;
        }

        subscription.request(1);
        synchronized (lockObject) {
            lastAccepted = input;
            lastRequestTime = Instant.now();
        }
        readTimeoutSinceLastRead = 0;
    }

    /**
     * Hands the batch to the shard consumer, retrying the same batch with backoff on failure.
     *
     * @return true if the batch was handed off; false if dispatching gave up (fatal error, shutdown, or interrupt).
     */
    private boolean dispatchWithRetry(RecordsRetrieved input) {
        long backoffMillis = dispatchRetryInitialBackoffMillis;
        int attempt = 0;
        while (true) {
            try {
                shardConsumer.handleInput(
                        input.processRecordsInput().toBuilder()
                                .cacheExitTime(Instant.now())
                                .build(),
                        subscription);
                return true;
            } catch (Exception e) {
                // KCL failed before delivering the batch to the record processor. Retry the same batch in place
                // rather than skipping it, so no data is lost. Ordering is preserved since the next batch is not
                // requested until this one succeeds.
                attempt++;
                emitDispatchFailureMetric(DISPATCH_FAILURE_METRIC);
                if (shardConsumer.isShutdownRequested()) {
                    // Without this, a batch that always fails (poison pill) would keep the shard from shutting down.
                    log.warn(
                            "{}: Failed to dispatch batch and shutdown was requested, not retrying. The batch was not"
                                    + " delivered and will be reprocessed from the last checkpoint.",
                            shardInfoId,
                            e);
                    return false;
                }
                log.warn(
                        "{}: Failed to dispatch batch (attempt {}), retrying the same batch in {} ms",
                        shardInfoId,
                        attempt,
                        backoffMillis,
                        e);
                try {
                    Thread.sleep(backoffMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("{}: Interrupted while waiting to retry dispatch, not retrying", shardInfoId);
                    return false;
                }
                backoffMillis = Math.min(backoffMillis * 2, dispatchRetryMaxBackoffMillis);
            } catch (Error e) {
                // Fatal error. Stop consuming this shard without advancing, so nothing can checkpoint past this
                // batch. The error is surfaced through healthCheck.
                log.error(
                        "{}: Fatal error while dispatching batch, stopping consumption of this shard", shardInfoId, e);
                synchronized (lockObject) {
                    dispatchFailure = e;
                }
                emitDispatchFailureMetric(DISPATCH_FATAL_ERROR_METRIC);
                return false;
            }
        }
    }

    /**
     * Permanently stops this subscriber: cancels the upstream subscription and prevents resubscribing.
     */
    private void stopDispatching() {
        synchronized (lockObject) {
            dispatchStopped = true;
            if (subscription != null) {
                subscription.cancel();
            }
        }
    }

    private void emitDispatchFailureMetric(String failureMetric) {
        final MetricsScope shardScope = MetricsUtil.createMetricsWithOperation(metricsFactory, METRICS_OPERATION);
        shardConsumer
                .shardInfo()
                .streamIdentifierSerOpt()
                .ifPresent(streamId ->
                        MetricsUtil.addStreamId(shardScope, StreamIdentifier.multiStreamInstance(streamId)));
        MetricsUtil.addShardId(shardScope, shardConsumer.shardInfo().shardId());
        final MetricsScope appScope =
                MetricsUtil.createMetricsWithOperation(metricsFactory, APPLICATION_TRACKER_OPERATION);
        for (MetricsScope scope : Arrays.asList(shardScope, appScope)) {
            scope.addData(failureMetric, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(scope);
        }
    }

    @Override
    public void onError(Throwable t) {
        synchronized (lockObject) {
            if (t instanceof RetryableRetrievalException && t.getMessage().contains("ReadTimeout")) {
                readTimeoutSinceLastRead++;
                if (readTimeoutSinceLastRead > readTimeoutsToIgnoreBeforeWarning) {
                    logOnErrorReadTimeoutWarning(t);
                }
            } else {
                logOnErrorWarning(t);
            }

            subscription.cancel();
            retrievalFailure = t;
        }
    }

    protected void logOnErrorWarning(Throwable t) {
        log.warn(
                "{}: onError().  Cancelling subscription, and marking self as failed. KCL will "
                        + "recreate the subscription as necessary to continue processing. Last successful request details -- {}",
                shardInfoId,
                recordsPublisher.getLastSuccessfulRequestDetails(),
                t);
    }

    protected void logOnErrorReadTimeoutWarning(Throwable t) {
        log.warn(
                "{}: onError().  Cancelling subscription, and marking self as failed. KCL will"
                        + " recreate the subscription as necessary to continue processing. If you"
                        + " are seeing this warning frequently consider increasing the SDK timeouts"
                        + " by providing an OverrideConfiguration to the kinesis client. Alternatively you"
                        + " can configure LifecycleConfig.readTimeoutsToIgnoreBeforeWarning to suppress"
                        + " intermittent ReadTimeout warnings. Last successful request details -- {}",
                shardInfoId,
                recordsPublisher.getLastSuccessfulRequestDetails(),
                t);
    }

    @Override
    public void onComplete() {
        log.debug("{}: onComplete(): Received onComplete.  Activity should be triggered externally", shardInfoId);
    }

    public void cancel() {
        if (subscription != null) {
            subscription.cancel();
        }
    }
}
