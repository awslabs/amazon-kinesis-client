/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;


import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.BlockedOnParentShardException;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.google.common.annotations.VisibleForTesting;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.checkpoint.RecordProcessorCheckpointer;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.metrics.MetricsCollectingTaskDecorator;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.retrieval.GetRecordsCache;

/**
 * Responsible for consuming data records of a (specified) shard.
 * The instance should be shutdown when we lose the primary responsibility for a shard.
 * A new instance should be created if the primary responsibility is reassigned back to this process.
 */
@RequiredArgsConstructor
@Getter(AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Slf4j
public class ShardConsumer {
    @NonNull
    private final ShardInfo shardInfo;
    @NonNull
    private final String streamName;
    @NonNull
    private final LeaseRefresher leaseRefresher;
    @NonNull
    private final ExecutorService executorService;
    @NonNull
    private final GetRecordsCache getRecordsCache;
    @NonNull
    private final RecordProcessor recordProcessor;
    @NonNull
    private final Checkpointer checkpoint;
    @NonNull
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final long parentShardPollIntervalMillis;
    private final long taskBackoffTimeMillis;
    @NonNull
    private final Optional<Long> logWarningForTaskAfterMillis;
    @NonNull
    private final AmazonKinesis amazonKinesis;
    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist;
    private final long listShardsBackoffTimeInMillis;
    private final int maxListShardsRetryAttempts;
    private final boolean shouldCallProcessRecordsEvenForEmptyRecordList;
    private final long idleTimeInMilliseconds;
    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;
    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;
    @NonNull
    private final ShardDetector shardDetector;
    @NonNull
    private final IMetricsFactory metricsFactory;

    private ITask currentTask;
    private long currentTaskSubmitTime;
    private Future<TaskResult> future;
    private boolean started = false;
    private Instant taskDispatchedAt;

    /*
     * Tracks current state. It is only updated via the consumeStream/shutdown APIs. Therefore we don't do
     * much coordination/synchronization to handle concurrent reads/updates.
     */
    private ConsumerStates.ConsumerState currentState = ConsumerStates.INITIAL_STATE;
    /*
     * Used to track if we lost the primary responsibility. Once set to true, we will start shutting down.
     * If we regain primary responsibility before shutdown is complete, Worker should create a new ShardConsumer object.
     */
    @Getter(AccessLevel.PUBLIC)
    private volatile ShutdownReason shutdownReason;
    private volatile ShutdownNotification shutdownNotification;

    private void start() {
        started = true;
        getRecordsCache.addDataArrivedListener(this::checkAndSubmitNextTask);
        checkAndSubmitNextTask();
    }
    /**
     * No-op if current task is pending, otherwise submits next task for this shard.
     * This method should NOT be called if the ShardConsumer is already in SHUTDOWN_COMPLETED state.
     * 
     * @return true if a new process task was submitted, false otherwise
     */
    @Synchronized
    public boolean consumeShard() {
        if (!started) {
            start();
        }
        if (taskDispatchedAt != null) {
            Duration taken = Duration.between(taskDispatchedAt, Instant.now());
            String commonMessage = String.format("Previous %s task still pending for shard %s since %s ago. ",
                    currentTask.taskType(), shardInfo.shardId(), taken);
            if (log.isDebugEnabled()) {
                log.debug("{} Not submitting new task.", commonMessage);
            }
            logWarningForTaskAfterMillis().ifPresent(value -> {
                if (taken.toMillis() > value) {
                    log.warn(commonMessage);
                }
            });
        }

        return true;
    }

    private boolean readyForNextTask() {
        return future == null || future.isCancelled() || future.isDone();
    }

    @Synchronized
    private boolean checkAndSubmitNextTask() {
        boolean submittedNewTask = false;
        if (readyForNextTask()) {
            TaskOutcome taskOutcome = TaskOutcome.NOT_COMPLETE;
            if (future != null && future.isDone()) {
                taskOutcome = determineTaskOutcome();
            }

            updateState(taskOutcome);
            ITask nextTask = getNextTask();
            if (nextTask != null) {
                nextTask.addTaskCompletedListener(this::handleTaskCompleted);
                currentTask = nextTask;
                try {
                    future = executorService.submit(currentTask);
                    taskDispatchedAt = Instant.now();
                    currentTaskSubmitTime = System.currentTimeMillis();
                    submittedNewTask = true;
                    log.debug("Submitted new {} task for shard {}", currentTask.taskType(), shardInfo.shardId());
                } catch (RejectedExecutionException e) {
                    log.info("{} task was not accepted for execution.", currentTask.taskType(), e);
                } catch (RuntimeException e) {
                    log.info("{} task encountered exception ", currentTask.taskType(), e);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("No new task to submit for shard {}, currentState {}",
                            shardInfo.shardId(),
                            currentState.toString());
                }
            }
        } else {
            final long timeElapsed = System.currentTimeMillis() - currentTaskSubmitTime;
            final String commonMessage = String.format("Previous %s task still pending for shard %s since %d ms ago.",
                    currentTask.taskType(), shardInfo.shardId(), timeElapsed);
            if (log.isDebugEnabled()) {
                log.debug("{} Not submitting new task.", commonMessage);
            }
            logWarningForTaskAfterMillis().ifPresent(value -> {
                if (timeElapsed > value) {
                    log.warn(commonMessage);
                }
            });
        }

        return submittedNewTask;
    }

    private boolean shouldDispatchNextTask() {
        return !isShutdown() || shutdownReason != null || getRecordsCache.hasResultAvailable();
    }

    @Synchronized
    private void handleTaskCompleted(ITask task) {
        if (future != null) {
            executorService.submit(() -> {
                resolveFuture();
                if (shouldDispatchNextTask()) {
                    checkAndSubmitNextTask();
                }
            });
        } else {
            log.error("Future wasn't set.  This shouldn't happen as polling should be disabled.  " +
                    "Will trigger next task check just in case");
            if (shouldDispatchNextTask()) {
                checkAndSubmitNextTask();
            }

        }

    }

    private enum TaskOutcome {
        SUCCESSFUL, END_OF_SHARD, NOT_COMPLETE, FAILURE
    }

    private void resolveFuture() {
        try {
            future.get();
        } catch (Exception e) {
            log.info("Ignoring caught exception '{}' exception during resolve.", e.getMessage());
        }
    }

    private TaskOutcome determineTaskOutcome() {
        try {
            TaskResult result = future.get();
            if (result.getException() == null) {
                if (result.isShardEndReached()) {
                    return TaskOutcome.END_OF_SHARD;
                }
                return TaskOutcome.SUCCESSFUL;
            }
            logTaskException(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // Setting future to null so we don't misinterpret task completion status in case of exceptions
            future = null;
        }
        return TaskOutcome.FAILURE;
    }

    private void logTaskException(TaskResult taskResult) {
        if (log.isDebugEnabled()) {
            Exception taskException = taskResult.getException();
            if (taskException instanceof BlockedOnParentShardException) {
                // No need to log the stack trace for this exception (it is very specific).
                log.debug("Shard {} is blocked on completion of parent shard.", shardInfo.shardId());
            } else {
                log.debug("Caught exception running {} task: ", currentTask.taskType(), taskResult.getException());
            }
        }
    }

    /**
     * Figure out next task to run based on current state, task, and shutdown context.
     *
     * @return Return next task to run
     */
    private ITask getNextTask() {
        ITask nextTask = currentState.createTask(this);

        if (nextTask == null) {
            return null;
        } else {
            return new MetricsCollectingTaskDecorator(nextTask, metricsFactory);
        }
    }

    /**
     * Note: This is a private/internal method with package level access solely for testing purposes.
     * Update state based on information about: task success, current state, and shutdown info.
     *
     * @param taskOutcome The outcome of the last task
     */
    void updateState(TaskOutcome taskOutcome) {
        if (taskOutcome == TaskOutcome.END_OF_SHARD) {
            markForShutdown(ShutdownReason.TERMINATE);
        }
        if (isShutdownRequested() && taskOutcome != TaskOutcome.FAILURE) {
            currentState = currentState.shutdownTransition(shutdownReason);
        } else if (taskOutcome == TaskOutcome.SUCCESSFUL) {
            if (currentState.getTaskType() == currentTask.taskType()) {
                currentState = currentState.successTransition();
            } else {
                log.error("Current State task type of '{}' doesn't match the current tasks type of '{}'.  This"
                        + " shouldn't happen, and indicates a programming error. Unable to safely transition to the"
                        + " next state.", currentState.getTaskType(), currentTask.taskType());
            }
        }
        //
        // Don't change state otherwise
        //

    }

    /**
     * Requests the shutdown of the this ShardConsumer. This should give the record processor a chance to checkpoint
     * before being shutdown.
     *
     * @param shutdownNotification used to signal that the record processor has been given the chance to shutdown.
     */
    public void notifyShutdownRequested(ShutdownNotification shutdownNotification) {
        this.shutdownNotification = shutdownNotification;
        markForShutdown(ShutdownReason.REQUESTED);
    }

    /**
     * Shutdown this ShardConsumer (including invoking the RecordProcessor shutdown API).
     * This is called by Worker when it loses responsibility for a shard.
     *
     * @return true if shutdown is complete (false if shutdown is still in progress)
     */
    @Synchronized
    public boolean beginShutdown() {
        markForShutdown(ShutdownReason.ZOMBIE);
        checkAndSubmitNextTask();

        return isShutdown();
    }

    synchronized void markForShutdown(ShutdownReason reason) {
        // ShutdownReason.ZOMBIE takes precedence over TERMINATE (we won't be able to save checkpoint at end of shard)
        if (shutdownReason == null || shutdownReason.canTransitionTo(reason)) {
            shutdownReason = reason;
        }
    }

    /**
     * Used (by Worker) to check if this ShardConsumer instance has been shutdown
     * RecordProcessor shutdown() has been invoked, as appropriate.
     *
     * @return true if shutdown is complete
     */
    public boolean isShutdown() {
        return currentState.isTerminal();
    }

    @VisibleForTesting
    public boolean isShutdownRequested() {
        return shutdownReason != null;
    }

    /**
     * Private/Internal method - has package level access solely for testing purposes.
     * 
     * @return the currentState
     */
    ConsumerStates.ShardConsumerState getCurrentState() {
        return currentState.getState();
    }
}
