/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Accessors(fluent = true)
class ShardConsumerSubscriber implements Subscriber<RecordsRetrieved> {

    private final RecordsPublisher recordsPublisher;
    private final Scheduler scheduler;
    private final int bufferSize;
    private final ShardConsumer shardConsumer;
    private final int readTimeoutsToIgnoreBeforeWarning;
    private volatile int readTimeoutSinceLastRead = 0;

    @VisibleForTesting
    final Object lockObject = new Object();
    private Instant lastRequestTime = null;
    private RecordsRetrieved lastAccepted = null;

    private Subscription subscription;
    @Getter
    private volatile Instant lastDataArrival;
    @Getter
    private volatile Throwable dispatchFailure;
    @Getter(AccessLevel.PACKAGE)
    private volatile Throwable retrievalFailure;

    ShardConsumerSubscriber(RecordsPublisher recordsPublisher, ExecutorService executorService, int bufferSize,
            ShardConsumer shardConsumer, int readTimeoutsToIgnoreBeforeWarning) {
        this.recordsPublisher = recordsPublisher;
        this.scheduler = Schedulers.from(executorService);
        this.bufferSize = bufferSize;
        this.shardConsumer = shardConsumer;
        this.readTimeoutsToIgnoreBeforeWarning = readTimeoutsToIgnoreBeforeWarning;
    }


    void startSubscriptions() {
        synchronized (lockObject) {
            if (lastAccepted != null) {
                recordsPublisher.restartFrom(lastAccepted);
            }
            Flowable.fromPublisher(recordsPublisher).subscribeOn(scheduler).observeOn(scheduler, true, bufferSize)
                    .subscribe(this);
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
                String logMessage = String.format("%s: Failure occurred in retrieval.  Restarting data requests",
                        shardConsumer.shardInfo().shardId());
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
                            "{}: Last request was dispatched at {}, but no response as of {} ({}).  Cancelling subscription, and restarting.",
                            shardConsumer.shardInfo().shardId(), lastRequestTime, now, timeSinceLastResponse);
                    cancel();
                    //
                    // Set the last request time to now, we specifically don't null it out since we want it to
                    // trigger a
                    // restart if the subscription still doesn't start producing.
                    //
                    lastRequestTime = Instant.now();
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
            shardConsumer.handleInput(input.processRecordsInput().toBuilder().cacheExitTime(Instant.now()).build(),
                    subscription);

        } catch (Throwable t) {
            log.warn("{}: Caught exception from handleInput", shardConsumer.shardInfo().shardId(), t);
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
                        + "recreate the subscription as neccessary to continue processing.",
                shardConsumer.shardInfo().shardId(), t);
    }

    protected void logOnErrorReadTimeoutWarning(Throwable t) {
        log.warn("{}: onError().  Cancelling subscription, and marking self as failed. KCL will"
                + " recreate the subscription as neccessary to continue processing. If you "
                + "are seeing this warning frequently consider increasing the SDK timeouts "
                + "by providing an OverrideConfiguration to the kinesis client. Alternatively you"
                + "can configure LifecycleConfig.readTimeoutsToIgnoreBeforeWarning to suppress"
                + "intermittant ReadTimeout warnings.", shardConsumer.shardInfo().shardId(), t);
    }

    @Override
    public void onComplete() {
        log.debug("{}: onComplete(): Received onComplete.  Activity should be triggered externally",
                shardConsumer.shardInfo().shardId());
    }

    public void cancel() {
        if (subscription != null) {
            subscription.cancel();
        }
    }
}
