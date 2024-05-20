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
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;

@Slf4j
@Accessors(fluent = true)
class ShardConsumerSubscriber implements Subscriber<RecordsRetrieved> {
    private final RecordsPublisher recordsPublisher;
    private final Scheduler scheduler;
    private final int bufferSize;
    private final ShardConsumer shardConsumer;
    private final int readTimeoutsToIgnoreBeforeWarning;
    private final String shardInfoId;
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

    @Getter
    private volatile Throwable dispatchFailure;

    @Getter(AccessLevel.PACKAGE)
    private volatile Throwable retrievalFailure;

    @Deprecated
    ShardConsumerSubscriber(
            RecordsPublisher recordsPublisher,
            ExecutorService executorService,
            int bufferSize,
            ShardConsumer shardConsumer) {
        this(
                recordsPublisher,
                executorService,
                bufferSize,
                shardConsumer,
                LifecycleConfig.DEFAULT_READ_TIMEOUTS_TO_IGNORE);
    }

    ShardConsumerSubscriber(
            RecordsPublisher recordsPublisher,
            ExecutorService executorService,
            int bufferSize,
            ShardConsumer shardConsumer,
            int readTimeoutsToIgnoreBeforeWarning) {
        this.recordsPublisher = recordsPublisher;
        this.scheduler = Schedulers.from(executorService);
        this.bufferSize = bufferSize;
        this.shardConsumer = shardConsumer;
        this.readTimeoutsToIgnoreBeforeWarning = readTimeoutsToIgnoreBeforeWarning;
        this.shardInfoId = ShardInfo.getLeaseKey(shardConsumer.shardInfo());
    }

    void startSubscriptions() {
        synchronized (lockObject) {
            // Setting the lastRequestTime to allow for health checks to restart subscriptions if they failed to
            // during initial try.
            lastRequestTime = Instant.now();
            if (lastAccepted != null) {
                recordsPublisher.restartFrom(lastAccepted);
            }
            Flowable.fromPublisher(recordsPublisher)
                    .subscribeOn(scheduler)
                    .observeOn(scheduler, true, bufferSize)
                    .subscribe(new ShardConsumerNotifyingSubscriber(this, recordsPublisher));
        }
    }

    Throwable healthCheck(long maxTimeBetweenRequests) {
        Throwable result = restartIfFailed();
        if (result == null) {
            restartIfRequestTimerExpired(maxTimeBetweenRequests);
        }
        return result;
    }

    Throwable getAndResetDispatchFailure() {
        synchronized (lockObject) {
            Throwable failure = dispatchFailure;
            dispatchFailure = null;
            return failure;
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
        try {
            synchronized (lockObject) {
                lastRequestTime = null;
            }
            lastDataArrival = Instant.now();
            shardConsumer.handleInput(
                    input.processRecordsInput().toBuilder()
                            .cacheExitTime(Instant.now())
                            .build(),
                    subscription);

        } catch (Throwable t) {
            log.warn("{}: Caught exception from handleInput", shardInfoId, t);
            synchronized (lockObject) {
                dispatchFailure = t;
            }
        } finally {
            subscription.request(1);
            synchronized (lockObject) {
                lastAccepted = input;
                lastRequestTime = Instant.now();
            }
        }

        readTimeoutSinceLastRead = 0;
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
