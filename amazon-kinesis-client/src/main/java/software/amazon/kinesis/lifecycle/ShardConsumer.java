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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

import org.reactivestreams.Subscription;

import com.google.common.annotations.VisibleForTesting;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.exceptions.internal.BlockedOnParentShardException;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.TaskExecutionListenerInput;
import software.amazon.kinesis.metrics.MetricsCollectingTaskDecorator;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;

/**
 * Responsible for consuming data records of a (specified) shard.
 * The instance should be shutdown when we lose the primary responsibility for a shard.
 * A new instance should be created if the primary responsibility is reassigned back to this process.
 */
@Getter(AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Slf4j
@KinesisClientInternalApi
public class ShardConsumer {

    public static final int MAX_TIME_BETWEEN_REQUEST_RESPONSE = 60 * 1000;
    private final RecordsPublisher recordsPublisher;
    private final ExecutorService executorService;
    private final ShardInfo shardInfo;
    private final ShardConsumerArgument shardConsumerArgument;
    @NonNull
    private final Optional<Long> logWarningForTaskAfterMillis;

    /**
     * @deprecated unused; to be removed in a "major" version bump
     */
    @Deprecated
    private final Function<ConsumerTask, ConsumerTask> taskMetricsDecorator;

    private final int bufferSize;
    private final TaskExecutionListener taskExecutionListener;
    private final String streamIdentifier;

    private ConsumerTask currentTask;
    private TaskOutcome taskOutcome;

    private CompletableFuture<Boolean> stateChangeFuture;
    private boolean needsInitialization = true;

    private volatile Instant taskDispatchedAt;
    private volatile boolean taskIsRunning = false;

    /**
     * Tracks current state. It is only updated via the consumeStream/shutdown APIs. Therefore we don't do
     * much coordination/synchronization to handle concurrent reads/updates.
     */
    private ConsumerState currentState;

    private final Object shutdownLock = new Object();
    @Getter(AccessLevel.PUBLIC)
    private volatile ShutdownReason shutdownReason;
    private volatile ShutdownNotification shutdownNotification;

    private final ShardConsumerSubscriber subscriber;

    private ProcessRecordsInput shardEndProcessRecordsInput;

    @Deprecated
    public ShardConsumer(RecordsPublisher recordsPublisher, ExecutorService executorService, ShardInfo shardInfo,
            Optional<Long> logWarningForTaskAfterMillis, ShardConsumerArgument shardConsumerArgument,
            TaskExecutionListener taskExecutionListener) {
        this(recordsPublisher, executorService, shardInfo, logWarningForTaskAfterMillis, shardConsumerArgument,
                ConsumerStates.INITIAL_STATE,
                ShardConsumer.metricsWrappingFunction(shardConsumerArgument.metricsFactory()), 8, taskExecutionListener,
                LifecycleConfig.DEFAULT_READ_TIMEOUTS_TO_IGNORE);
    }

    public ShardConsumer(RecordsPublisher recordsPublisher, ExecutorService executorService, ShardInfo shardInfo,
            Optional<Long> logWarningForTaskAfterMillis, ShardConsumerArgument shardConsumerArgument,
            TaskExecutionListener taskExecutionListener, int readTimeoutsToIgnoreBeforeWarning) {
        this(recordsPublisher, executorService, shardInfo, logWarningForTaskAfterMillis, shardConsumerArgument,
                ConsumerStates.INITIAL_STATE,
                ShardConsumer.metricsWrappingFunction(shardConsumerArgument.metricsFactory()), 8, taskExecutionListener,
                readTimeoutsToIgnoreBeforeWarning);
    }

    @Deprecated
    public ShardConsumer(RecordsPublisher recordsPublisher, ExecutorService executorService, ShardInfo shardInfo,
            Optional<Long> logWarningForTaskAfterMillis, ShardConsumerArgument shardConsumerArgument,
            ConsumerState initialState, Function<ConsumerTask, ConsumerTask> taskMetricsDecorator, int bufferSize,
            TaskExecutionListener taskExecutionListener) {
        this(recordsPublisher, executorService, shardInfo, logWarningForTaskAfterMillis, shardConsumerArgument,
                initialState, taskMetricsDecorator, bufferSize, taskExecutionListener,
                LifecycleConfig.DEFAULT_READ_TIMEOUTS_TO_IGNORE);
    }

    //
    // TODO: Make bufferSize configurable
    //
    public ShardConsumer(RecordsPublisher recordsPublisher, ExecutorService executorService, ShardInfo shardInfo,
            Optional<Long> logWarningForTaskAfterMillis, ShardConsumerArgument shardConsumerArgument,
            ConsumerState initialState, Function<ConsumerTask, ConsumerTask> taskMetricsDecorator, int bufferSize,
            TaskExecutionListener taskExecutionListener, int readTimeoutsToIgnoreBeforeWarning) {
        this.recordsPublisher = recordsPublisher;
        this.executorService = executorService;
        this.shardInfo = shardInfo;
        this.streamIdentifier = shardInfo.streamConfig().streamIdentifier().serialize();
        this.shardConsumerArgument = shardConsumerArgument;
        this.logWarningForTaskAfterMillis = logWarningForTaskAfterMillis;
        this.taskExecutionListener = taskExecutionListener;
        this.currentState = initialState;
        this.taskMetricsDecorator = taskMetricsDecorator;
        subscriber = new ShardConsumerSubscriber(recordsPublisher, executorService, bufferSize, this,
                readTimeoutsToIgnoreBeforeWarning);
        this.bufferSize = bufferSize;

        if (this.shardInfo.isCompleted()) {
            markForShutdown(ShutdownReason.SHARD_END);
        }
    }

    synchronized void handleInput(ProcessRecordsInput input, Subscription subscription) {
        if (isShutdownRequested()) {
            subscription.cancel();
            return;
        }
        processData(input);
        if (taskOutcome == TaskOutcome.END_OF_SHARD) {
            markForShutdown(ShutdownReason.SHARD_END);
            shardEndProcessRecordsInput = input;
            subscription.cancel();
            return;
        }
        subscription.request(1);
    }

    public void executeLifecycle() {
        if (isShutdown()) {
            return;
        }
        if (stateChangeFuture != null && !stateChangeFuture.isDone()) {
            return;
        }
        try {
            if (isShutdownRequested()) {
                stateChangeFuture = shutdownComplete();
            } else if (needsInitialization) {
                if (stateChangeFuture != null) {
                    if (stateChangeFuture.get()) {
                        // Task rejection during the subscribe() call will not be propagated back as it not executed
                        // in the context of the Scheduler thread. Hence we should not assume the subscription will
                        // always be successful.
                        subscribe();
                        needsInitialization = false;
                    }
                }
                stateChangeFuture = initializeComplete();
            }
        } catch (InterruptedException e) {
            //
            // Ignored should be handled by scheduler
            //
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (RejectedExecutionException e) {
            // It is possible the tasks submitted to the executor service by the Scheduler thread might get rejected
            // due to various reasons. Such failed executions must be captured and marked as failure to prevent
            // the state transitions.
            taskOutcome = TaskOutcome.FAILURE;
        }

        if (ConsumerStates.ShardConsumerState.PROCESSING.equals(currentState.state())) {
            Throwable t = healthCheck();
            if (t instanceof Error) {
                throw (Error) t;
            }
        }
    }

    @VisibleForTesting
    Throwable healthCheck() {
        logNoDataRetrievedAfterTime();
        logLongRunningTask();
        Throwable failure = subscriber.healthCheck(MAX_TIME_BETWEEN_REQUEST_RESPONSE);

        if (failure != null) {
            return failure;
        }
        Throwable dispatchFailure = subscriber.getAndResetDispatchFailure();
        if (dispatchFailure != null) {
            log.warn("{} : Exception occurred while dispatching incoming data.  The incoming data has been skipped",
                    streamIdentifier, dispatchFailure);
            return dispatchFailure;
        }

        return null;
    }

    Duration taskRunningTime() {
        if (taskDispatchedAt != null && taskIsRunning) {
            return Duration.between(taskDispatchedAt, Instant.now());
        }
        return null;
    }

    String longRunningTaskMessage(Duration taken) {
        if (taken != null) {
            return String.format("Previous %s task still pending for shard %s since %s ago. ", currentTask.taskType(),
                    shardInfo.shardId(), taken);
        }
        return null;
    }

    private void logNoDataRetrievedAfterTime() {
        logWarningForTaskAfterMillis.ifPresent(value -> {
            Instant lastDataArrival = subscriber.lastDataArrival();
            if (lastDataArrival != null) {
                Instant now = Instant.now();
                Duration timeSince = Duration.between(subscriber.lastDataArrival(), now);
                if (timeSince.toMillis() > value) {
                    log.warn("{} : Last time data arrived: {} ({})", streamIdentifier, lastDataArrival, timeSince);
                }
            }
        });
    }

    private void logLongRunningTask() {
        Duration taken = taskRunningTime();

        if (taken != null) {
            String message = longRunningTaskMessage(taken);
            if (log.isDebugEnabled()) {
                log.debug("{} : {} Not submitting new task.", streamIdentifier, message);
            }
            logWarningForTaskAfterMillis.ifPresent(value -> {
                if (taken.toMillis() > value) {
                    log.warn("{} : {}", streamIdentifier, message);
                }
            });
        }
    }

    @VisibleForTesting
    void subscribe() {
        subscriber.startSubscriptions();
    }

    @VisibleForTesting
    synchronized CompletableFuture<Boolean> initializeComplete() {
        if (taskOutcome != null) {
            updateState(taskOutcome);
        }
        if (currentState.state() == ConsumerStates.ShardConsumerState.PROCESSING) {
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.supplyAsync(() -> {
            if (isShutdownRequested()) {
                throw new IllegalStateException("Shutdown requested while initializing");
            }
            executeTask(null);
            if (isShutdownRequested()) {
                throw new IllegalStateException("Shutdown requested while initializing");
            }
            return false;
        }, executorService);
    }

    @VisibleForTesting
    CompletableFuture<Boolean> shutdownComplete() {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (this) {
                if (taskOutcome != null) {
                    updateState(taskOutcome);
                } else {
                    //
                    // ShardConsumer has been asked to shutdown before the first task even had a chance to run.
                    // In this case generate a successful task outcome, and allow the shutdown to continue.
                    // This should only happen if the lease was lost before the initial state had a chance to run.
                    //
                    updateState(TaskOutcome.SUCCESSFUL);
                }
                if (isShutdown()) {
                    return true;
                }

                executeTask(shardEndProcessRecordsInput);
                return false;
            }
        }, executorService);
    }

    private synchronized void processData(ProcessRecordsInput input) {
        executeTask(input);
    }

    private synchronized void executeTask(ProcessRecordsInput input) {
        TaskExecutionListenerInput taskExecutionListenerInput = TaskExecutionListenerInput.builder()
                .shardInfo(shardInfo).taskType(currentState.taskType()).build();
        taskExecutionListener.beforeTaskExecution(taskExecutionListenerInput);
        ConsumerTask task = currentState.createTask(shardConsumerArgument, ShardConsumer.this, input);
        if (task != null) {
            taskDispatchedAt = Instant.now();
            currentTask = task;
            taskIsRunning = true;
            TaskResult result;
            try {
                result = task.call();
            } finally {
                taskIsRunning = false;
            }
            taskOutcome = resultToOutcome(result);
            taskExecutionListenerInput = taskExecutionListenerInput.toBuilder().taskOutcome(taskOutcome).build();
        }
        taskExecutionListener.afterTaskExecution(taskExecutionListenerInput);
    }

    private TaskOutcome resultToOutcome(TaskResult result) {
        if (result.getException() == null) {
            if (result.isShardEndReached()) {
                return TaskOutcome.END_OF_SHARD;
            }
            return TaskOutcome.SUCCESSFUL;
        }
        logTaskException(result);
        return TaskOutcome.FAILURE;
    }

    private synchronized void updateState(TaskOutcome outcome) {
        ConsumerState nextState = currentState;
        switch (outcome) {
        case SUCCESSFUL:
            nextState = currentState.successTransition();
            break;
        case END_OF_SHARD:
            markForShutdown(ShutdownReason.SHARD_END);
            break;
        case FAILURE:
            nextState = currentState.failureTransition();
            break;
        default:
            log.error("{} : No handler for outcome of {}", streamIdentifier, outcome.name());
            nextState = currentState.failureTransition();
            break;
        }

        nextState = handleShutdownTransition(outcome, nextState);

        currentState = nextState;
    }

    private ConsumerState handleShutdownTransition(TaskOutcome outcome, ConsumerState nextState) {
        synchronized (shutdownLock) {
            if (isShutdownRequested() && outcome != TaskOutcome.FAILURE) {
                return currentState.shutdownTransition(shutdownReason);
            }
            return nextState;
        }
    }

    private void logTaskException(TaskResult taskResult) {
        if (log.isDebugEnabled()) {
            Exception taskException = taskResult.getException();
            if (taskException instanceof BlockedOnParentShardException) {
                // No need to log the stack trace for this exception (it is very specific).
                log.debug("{} : Shard {} is blocked on completion of parent shard.", streamIdentifier, shardInfo.shardId());
            } else {
                log.debug("{} : Caught exception running {} task: ", streamIdentifier, currentTask.taskType(), taskResult.getException());
            }
        }
    }

    /**
     * Requests the shutdown of the this ShardConsumer. This should give the record processor a chance to checkpoint
     * before being shutdown.
     *
     * @param shutdownNotification
     *            used to signal that the record processor has been given the chance to shutdown.
     */
    public void gracefulShutdown(ShutdownNotification shutdownNotification) {
        if (subscriber != null) {
            subscriber.cancel();
        }
        this.shutdownNotification = shutdownNotification;
        markForShutdown(ShutdownReason.REQUESTED);
    }

    /**
     * Shutdown this ShardConsumer (including invoking the ShardRecordProcessor shutdown API).
     * This is called by Worker when it loses responsibility for a shard.
     *
     * @return true if shutdown is complete (false if shutdown is still in progress)
     */
    public boolean leaseLost() {
        log.debug("{} : Shutdown({}): Lease lost triggered.", streamIdentifier, shardInfo.shardId());
        if (subscriber != null) {
            subscriber.cancel();
            log.debug("{} : Shutdown({}): Subscriber cancelled.", streamIdentifier, shardInfo.shardId());
        }
        markForShutdown(ShutdownReason.LEASE_LOST);
        return isShutdown();
    }

    void markForShutdown(ShutdownReason reason) {
        synchronized (shutdownLock) {
            //
            // ShutdownReason.LEASE_LOST takes precedence over SHARD_END
            // (we won't be able to save checkpoint at end of shard)
            //
            if (shutdownReason == null || shutdownReason.canTransitionTo(reason)) {
                shutdownReason = reason;
            }
        }
    }

    /**
     * Used (by Worker) to check if this ShardConsumer instance has been shutdown
     * ShardRecordProcessor shutdown() has been invoked, as appropriate.
     *
     * @return true if shutdown is complete
     */
    public boolean isShutdown() {
        return currentState.isTerminal();
    }

    @VisibleForTesting
    public boolean isShutdownRequested() {
        synchronized (shutdownLock) {
            return shutdownReason != null;
        }
    }

    /**
     * Default task wrapping function for metrics
     *
     * @param metricsFactory
     *            the factory used for reporting metrics
     * @return a function that will wrap the task with a metrics reporter
     */
    private static Function<ConsumerTask, ConsumerTask> metricsWrappingFunction(MetricsFactory metricsFactory) {
        return (task) -> {
            if (task == null) {
                return null;
            } else {
                return new MetricsCollectingTaskDecorator(task, metricsFactory);
            }
        };
    }

}
