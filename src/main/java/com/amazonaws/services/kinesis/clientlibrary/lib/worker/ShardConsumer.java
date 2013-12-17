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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.BlockedOnParentShardException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

/**
 * Responsible for consuming data records of a (specified) shard.
 * The instance should be shutdown when we lose the primary responsibility for a shard.
 * A new instance should be created if the primary responsibility is reassigned back to this process.
 */
class ShardConsumer {

    /**
     * Enumerates processing states when working on a shard.
     */
    enum ShardConsumerState {
        WAITING_ON_PARENT_SHARDS, INITIALIZING, PROCESSING, SHUTTING_DOWN, SHUTDOWN_COMPLETE;
    }

    private static final Log LOG = LogFactory.getLog(ShardConsumer.class);

    private final StreamConfig streamConfig;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ExecutorService executorService;
    private final ShardInfo shardInfo;
    private final KinesisDataFetcher dataFetcher;
    private final IMetricsFactory metricsFactory;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private ICheckpoint checkpoint;
    // Backoff time when polling to check if application has finished processing parent shards
    private final long parentShardPollIntervalMillis;
    private final boolean cleanupLeasesOfCompletedShards;
    private final long taskBackoffTimeMillis;

    private ITask currentTask;
    private long currentTaskSubmitTime;
    private Future<TaskResult> future;

    /*
     * Tracks current state. It is only updated via the consumeStream/shutdown APIs. Therefore we don't do
     * much coordination/synchronization to handle concurrent reads/updates.
     */
    private ShardConsumerState currentState = ShardConsumerState.WAITING_ON_PARENT_SHARDS;
    /*
     * Used to track if we lost the primary responsibility. Once set to true, we will start shutting down.
     * If we regain primary responsibility before shutdown is complete, Worker should create a new ShardConsumer object.
     */
    private boolean beginShutdown;
    private ShutdownReason shutdownReason;


    /**
     * @param shardInfo Shard information
     * @param streamConfig Stream configuration to use
     * @param checkpoint Checkpoint tracker
     * @param recordProcessor Record processor used to process the data records for the shard
     * @param leaseManager Used to create leases for new shards
     * @param parentShardPollIntervalMillis Wait for this long if parent shards are not done (or we get an exception)
     * @param executorService ExecutorService used to execute process tasks for this shard
     * @param metricsFactory IMetricsFactory used to construct IMetricsScopes for this shard
     * @param backoffTimeMillis backoff interval when we encounter exceptions
     */
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    ShardConsumer(ShardInfo shardInfo,
            StreamConfig streamConfig,
            ICheckpoint checkpoint,
            IRecordProcessor recordProcessor,
            ILeaseManager<KinesisClientLease> leaseManager,
            long parentShardPollIntervalMillis,
            boolean cleanupLeasesOfCompletedShards,
            ExecutorService executorService,
            IMetricsFactory metricsFactory,
            long backoffTimeMillis) {
        this.streamConfig = streamConfig;
        this.recordProcessor = recordProcessor;
        this.executorService = executorService;
        this.shardInfo = shardInfo;
        this.checkpoint = checkpoint;
        this.recordProcessorCheckpointer = new RecordProcessorCheckpointer(shardInfo, checkpoint);
        this.dataFetcher = new KinesisDataFetcher(streamConfig.getStreamProxy(), shardInfo);
        this.leaseManager = leaseManager;
        this.metricsFactory = metricsFactory;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
        this.taskBackoffTimeMillis = backoffTimeMillis;
    }

    /**
     * No-op if current task is pending, otherwise submits next task for this shard.
     * This method should NOT be called if the ShardConsumer is already in SHUTDOWN_COMPLETED state.
     * 
     * @return true if a new process task was submitted, false otherwise
     */
    synchronized boolean consumeShard() {
        return checkAndSubmitNextTask();
    }

    // CHECKSTYLE:OFF CyclomaticComplexity
    private synchronized boolean checkAndSubmitNextTask() {
        // Task completed successfully (without exceptions)
        boolean taskCompletedSuccessfully = false;
        boolean submittedNewTask = false;
        if ((future == null) || future.isCancelled() || future.isDone()) {
            if ((future != null) && future.isDone()) {
                try {
                    TaskResult result = future.get();
                    if (result.getException() == null) {
                        taskCompletedSuccessfully = true;
                        if (result.isShardEndReached()) {
                            markForShutdown(ShutdownReason.TERMINATE);
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            Exception taskException = result.getException();
                            if (taskException instanceof BlockedOnParentShardException) {
                                // No need to log the stack trace for this exception (it is very specific).
                                LOG.debug("Shard " + shardInfo.getShardId()
                                        + " is blocked on completion of parent shard.");
                            } else {
                                LOG.debug("Caught exception running " + currentTask.getTaskType() + " task: ",
                                        result.getException());                                
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(currentTask.getTaskType() + " task was interrupted: ", e);
                    }
                } catch (ExecutionException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(currentTask.getTaskType() + " task encountered execution exception: ", e);
                    }
                }
            }
            updateState(taskCompletedSuccessfully);
            ITask nextTask = getNextTask();
            if (nextTask != null) {
                currentTask = nextTask;
                future = executorService.submit(currentTask);
                currentTaskSubmitTime = System.currentTimeMillis();
                submittedNewTask = true;
                LOG.debug("Submitted new " + currentTask.getTaskType() + " task for shard " + shardInfo.getShardId());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("No new task to submit for shard %s, currentState %s",
                            shardInfo.getShardId(),
                            currentState.toString()));
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Previous " + currentTask.getTaskType() + " task still pending for shard "
                        + shardInfo.getShardId() + " since " + (System.currentTimeMillis() - currentTaskSubmitTime)
                        + " ms ago" + ".  Not submitting new task.");
            }
        }

        return submittedNewTask;
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    /**
     * Shutdown this ShardConsumer (including invoking the RecordProcessor shutdown API).
     * This is called by Worker when it loses responsibility for a shard.
     * @return true if shutdown is complete (false if shutdown is still in progress)
     */
    synchronized boolean beginShutdown() {
        if (currentState != ShardConsumerState.SHUTDOWN_COMPLETE) {
            markForShutdown(ShutdownReason.ZOMBIE);
            checkAndSubmitNextTask();
        }
        return isShutdown();
    }
    
    synchronized void markForShutdown(ShutdownReason reason) {
        beginShutdown = true;
        // ShutdownReason.ZOMBIE takes precedence over TERMINATE (we won't be able to save checkpoint at end of shard)
        if ((shutdownReason == null) || (shutdownReason == ShutdownReason.TERMINATE)) {
            shutdownReason = reason;
        }
    }

    /**
     * Used (by Worker) to check if this ShardConsumer instance has been shutdown
     * RecordProcessor shutdown() has been invoked, as appropriate.
     * 
     * @return true if shutdown is complete
     */
    boolean isShutdown() {
        return currentState == ShardConsumerState.SHUTDOWN_COMPLETE;
    }
    
    /**
     * @return the shutdownReason
     */
    ShutdownReason getShutdownReason() {
        return shutdownReason;
    }

    /**
     * Figure out next task to run based on current state, task, and shutdown context.
     * @return Return next task to run
     */
    private ITask getNextTask() {
        ITask nextTask = null;
        switch (currentState) {
            case WAITING_ON_PARENT_SHARDS:
                nextTask = new BlockOnParentShardTask(shardInfo, leaseManager, parentShardPollIntervalMillis);
                break;
            case INITIALIZING:
                nextTask =
                        new InitializeTask(shardInfo,
                                recordProcessor,
                                checkpoint,
                                recordProcessorCheckpointer,
                                dataFetcher,
                                taskBackoffTimeMillis);
                break;
            case PROCESSING:
                nextTask =
                        new ProcessTask(shardInfo,
                                streamConfig,
                                recordProcessor,
                                recordProcessorCheckpointer,
                                dataFetcher,
                                taskBackoffTimeMillis);
                break;
            case SHUTTING_DOWN:
                nextTask =
                        new ShutdownTask(shardInfo,
                                recordProcessor,
                                recordProcessorCheckpointer,
                                shutdownReason,
                                streamConfig.getStreamProxy(),
                                streamConfig.getInitialPositionInStream(),
                                cleanupLeasesOfCompletedShards,
                                leaseManager,
                                taskBackoffTimeMillis);
                break;
            case SHUTDOWN_COMPLETE:
                break;
            default:
                break;
        }

        if (nextTask == null) {
            return null;
        } else {
            return new MetricsCollectingTaskDecorator(nextTask, metricsFactory);
        }
    }

    /**
     * Note: This is a private/internal method with package level access solely for testing purposes.
     * Update state based on information about: task success, current state, and shutdown info.
     * @param taskCompletedSuccessfully Whether (current) task completed successfully.
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    void updateState(boolean taskCompletedSuccessfully) {
        switch (currentState) {
            case WAITING_ON_PARENT_SHARDS:
                if (taskCompletedSuccessfully && TaskType.BLOCK_ON_PARENT_SHARDS.equals(currentTask.getTaskType())) {
                    if (beginShutdown) {
                        currentState = ShardConsumerState.SHUTTING_DOWN;
                    } else {
                        currentState = ShardConsumerState.INITIALIZING;
                    }
                } else if ((currentTask == null) && beginShutdown) {
                    currentState = ShardConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case INITIALIZING:
                if (taskCompletedSuccessfully && TaskType.INITIALIZE.equals(currentTask.getTaskType())) {
                    if (beginShutdown) {
                        currentState = ShardConsumerState.SHUTTING_DOWN;
                    } else {
                        currentState = ShardConsumerState.PROCESSING;
                    }
                } else if ((currentTask == null) && beginShutdown) {
                    currentState = ShardConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case PROCESSING:
                if (taskCompletedSuccessfully && TaskType.PROCESS.equals(currentTask.getTaskType())) {
                    if (beginShutdown) {
                        currentState = ShardConsumerState.SHUTTING_DOWN;
                    } else {
                        currentState = ShardConsumerState.PROCESSING;
                    }
                }
                break;
            case SHUTTING_DOWN:
                if (currentTask == null
                        || (taskCompletedSuccessfully && TaskType.SHUTDOWN.equals(currentTask.getTaskType()))) {
                    currentState = ShardConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case SHUTDOWN_COMPLETE:
                break;
            default:
                LOG.error("Unexpected state: " + currentState);
                break;
        }
    }

    // CHECKSTYLE:ON CyclomaticComplexity

    /**
     * Private/Internal method - has package level access solely for testing purposes.
     * 
     * @return the currentState
     */
    ShardConsumerState getCurrentState() {
        return currentState;
    }

}
