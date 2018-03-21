/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.kinesis.coordinator;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.google.common.annotations.VisibleForTesting;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.leases.KinesisClientLibLeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardPrioritization;
import software.amazon.kinesis.leases.ShardSyncTask;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.metrics.CWMetricsFactory;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.metrics.MetricsCollectingTaskDecorator;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ICheckpoint;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.ProcessorFactory;
import software.amazon.kinesis.processor.v2.IRecordProcessor;
import software.amazon.kinesis.retrieval.IKinesisProxy;
import software.amazon.kinesis.retrieval.RetrievalConfig;

/**
 *
 */
@Getter
@Slf4j
public class Scheduler implements Runnable {
    private static final int MAX_INITIALIZATION_ATTEMPTS = 20;
    private WorkerLog wlog = new WorkerLog();

    private final CheckpointConfig checkpointConfig;
    private final CoordinatorConfig coordinatorConfig;
    private final LeaseManagementConfig leaseManagementConfig;
    private final LifecycleConfig lifecycleConfig;
    private final MetricsConfig metricsConfig;
    private final ProcessorConfig processorConfig;
    private final RetrievalConfig retrievalConfig;
    // TODO: Should be removed.
    private final KinesisClientLibConfiguration config;

    private final String applicationName;
    private final ICheckpoint checkpoint;
    private final long idleTimeInMilliseconds;
    // Backoff time when polling to check if application has finished processing
    // parent shards
    private final long parentShardPollIntervalMillis;
    private final ExecutorService executorService;
    // private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private final ShardSyncTaskManager controlServer;
    private final ShardPrioritization shardPrioritization;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist;
    private final GracefulShutdownCoordinator gracefulShutdownCoordinator;
    private final WorkerStateChangeListener workerStateChangeListener;
    private final InitialPositionInStreamExtended initialPosition;
    private final IMetricsFactory metricsFactory;
    private final long failoverTimeMillis;
    private final ProcessorFactory processorFactory;
    private final long taskBackoffTimeMillis;
    private final Optional<Integer> retryGetRecordsInSeconds;
    private final Optional<Integer> maxGetRecordsThreadPool;

    private final StreamConfig streamConfig;

    // Holds consumers for shards the worker is currently tracking. Key is shard
    // info, value is ShardConsumer.
    private ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap = new ConcurrentHashMap<ShardInfo, ShardConsumer>();

    private volatile boolean shutdown;
    private volatile long shutdownStartTimeMillis;
    private volatile boolean shutdownComplete = false;

    /**
     * Used to ensure that only one requestedShutdown is in progress at a time.
     */
    private Future<Boolean> gracefulShutdownFuture;
    @VisibleForTesting
    protected boolean gracefuleShutdownStarted = false;

    public Scheduler(@NonNull final CheckpointConfig checkpointConfig,
                     @NonNull final CoordinatorConfig coordinatorConfig,
                     @NonNull final LeaseManagementConfig leaseManagementConfig,
                     @NonNull final LifecycleConfig lifecycleConfig,
                     @NonNull final MetricsConfig metricsConfig,
                     @NonNull final ProcessorConfig processorConfig,
                     @NonNull final RetrievalConfig retrievalConfig,
                     @NonNull final KinesisClientLibConfiguration config) {
        this.checkpointConfig = checkpointConfig;
        this.coordinatorConfig = coordinatorConfig;
        this.leaseManagementConfig = leaseManagementConfig;
        this.lifecycleConfig = lifecycleConfig;
        this.metricsConfig = metricsConfig;
        this.processorConfig = processorConfig;
        this.retrievalConfig = retrievalConfig;
        this.config = config;

        this.applicationName = this.coordinatorConfig.applicationName();
        this.checkpoint = this.checkpointConfig.checkpointFactory().createCheckpoint();
        this.idleTimeInMilliseconds = this.retrievalConfig.idleTimeBetweenReadsInMillis();
        this.parentShardPollIntervalMillis = this.coordinatorConfig.parentShardPollIntervalMillis();
        this.executorService = this.coordinatorConfig.coordinatorFactory().createExecutorService();
        this.leaseCoordinator =
                this.leaseManagementConfig.leaseManagementFactory().createKinesisClientLibLeaseCoordinator();
        this.controlServer = this.leaseManagementConfig.leaseManagementFactory().createShardSyncTaskManager();
        this.shardPrioritization = this.coordinatorConfig.shardPrioritization();
        this.cleanupLeasesUponShardCompletion = this.leaseManagementConfig.cleanupLeasesUponShardCompletion();
        this.skipShardSyncAtWorkerInitializationIfLeasesExist =
                this.coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist();
        this.gracefulShutdownCoordinator =
                this.coordinatorConfig.coordinatorFactory().createGracefulShutdownCoordinator();
        this.workerStateChangeListener = this.coordinatorConfig.coordinatorFactory().createWorkerStateChangeListener();
        this.initialPosition =
                InitialPositionInStreamExtended.newInitialPosition(this.retrievalConfig.initialPositionInStream());
        this.metricsFactory = this.coordinatorConfig.metricsFactory();
        this.failoverTimeMillis = this.leaseManagementConfig.failoverTimeMillis();
        this.processorFactory = this.processorConfig.processorFactory();
        this.taskBackoffTimeMillis = this.lifecycleConfig.taskBackoffTimeMillis();
        this.retryGetRecordsInSeconds = this.retrievalConfig.retryGetRecordsInSeconds();
        this.maxGetRecordsThreadPool = this.retrievalConfig.maxGetRecordsThreadPool();

        this.streamConfig = createStreamConfig(this.retrievalConfig.retrievalFactory().createKinesisProxy(),
                this.retrievalConfig.maxRecords(),
                this.idleTimeInMilliseconds,
                this.processorConfig.callProcessRecordsEvenForEmptyRecordList(),
                this.checkpointConfig.validateSequenceNumberBeforeCheckpointing(),
                this.initialPosition);
    }

    /**
     * Start consuming data from the stream, and pass it to the application record processors.
     */
    @Override
    public void run() {
        if (shutdown) {
            return;
        }

        try {
            initialize();
            log.info("Initialization complete. Starting worker loop.");
        } catch (RuntimeException e1) {
            log.error("Unable to initialize after {} attempts. Shutting down.", MAX_INITIALIZATION_ATTEMPTS, e1);
            shutdown();
        }

        while (!shouldShutdown()) {
            runProcessLoop();
        }

        finalShutdown();
        log.info("Worker loop is complete. Exiting from worker.");
    }

    private void initialize() {
        workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.INITIALIZING);
        boolean isDone = false;
        Exception lastException = null;

        for (int i = 0; (!isDone) && (i < MAX_INITIALIZATION_ATTEMPTS); i++) {
            try {
                log.info("Initialization attempt {}", (i + 1));
                log.info("Initializing LeaseCoordinator");
                leaseCoordinator.initialize();

                TaskResult result = null;
                if (!skipShardSyncAtWorkerInitializationIfLeasesExist
                        || leaseCoordinator.getLeaseManager().isLeaseTableEmpty()) {
                    log.info("Syncing Kinesis shard info");
                    ShardSyncTask shardSyncTask = new ShardSyncTask(streamConfig.getStreamProxy(),
                            leaseCoordinator.getLeaseManager(), initialPosition, cleanupLeasesUponShardCompletion,
                            leaseManagementConfig.ignoreUnexpectedChildShards(), 0L);
                    result = new MetricsCollectingTaskDecorator(shardSyncTask, metricsFactory).call();
                } else {
                    log.info("Skipping shard sync per config setting (and lease table is not empty)");
                }

                if (result == null || result.getException() == null) {
                    if (!leaseCoordinator.isRunning()) {
                        log.info("Starting LeaseCoordinator");
                        leaseCoordinator.start();
                    } else {
                        log.info("LeaseCoordinator is already running. No need to start it.");
                    }
                    isDone = true;
                } else {
                    lastException = result.getException();
                }
            } catch (LeasingException e) {
                log.error("Caught exception when initializing LeaseCoordinator", e);
                lastException = e;
            } catch (Exception e) {
                lastException = e;
            }

            try {
                Thread.sleep(parentShardPollIntervalMillis);
            } catch (InterruptedException e) {
                log.debug("Sleep interrupted while initializing worker.");
            }
        }

        if (!isDone) {
            throw new RuntimeException(lastException);
        }
        workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.STARTED);
    }

    @VisibleForTesting
    void runProcessLoop() {
        try {
            boolean foundCompletedShard = false;
            Set<ShardInfo> assignedShards = new HashSet<>();
            for (ShardInfo shardInfo : getShardInfoForAssignments()) {
                ShardConsumer shardConsumer = createOrGetShardConsumer(shardInfo, processorFactory);
                if (shardConsumer.isShutdown() && shardConsumer.getShutdownReason().equals(ShutdownReason.TERMINATE)) {
                    foundCompletedShard = true;
                } else {
                    shardConsumer.consumeShard();
                }
                assignedShards.add(shardInfo);
            }

            if (foundCompletedShard) {
                controlServer.syncShardAndLeaseInfo(null);
            }

            // clean up shard consumers for unassigned shards
            cleanupShardConsumers(assignedShards);

            wlog.info("Sleeping ...");
            Thread.sleep(idleTimeInMilliseconds);
        } catch (Exception e) {
            log.error("Worker.run caught exception, sleeping for {} milli seconds!",
                    String.valueOf(idleTimeInMilliseconds), e);
            try {
                Thread.sleep(idleTimeInMilliseconds);
            } catch (InterruptedException ex) {
                log.info("Worker: sleep interrupted after catching exception ", ex);
            }
        }
        wlog.resetInfoLogging();
    }

    /**
     * Returns whether worker can shutdown immediately. Note that this method is called from Worker's {{@link #run()}
     * method before every loop run, so method must do minimum amount of work to not impact shard processing timings.
     *
     * @return Whether worker should shutdown immediately.
     */
    @VisibleForTesting
    boolean shouldShutdown() {
        if (executorService.isShutdown()) {
            log.error("Worker executor service has been shutdown, so record processors cannot be shutdown.");
            return true;
        }
        if (shutdown) {
            if (shardInfoShardConsumerMap.isEmpty()) {
                log.info("All record processors have been shutdown successfully.");
                return true;
            }
            if ((System.currentTimeMillis() - shutdownStartTimeMillis) >= failoverTimeMillis) {
                log.info("Lease failover time is reached, so forcing shutdown.");
                return true;
            }
        }
        return false;
    }

    /**
     * Signals worker to shutdown. Worker will try initiating shutdown of all record processors. Note that if executor
     * services were passed to the worker by the user, worker will not attempt to shutdown those resources.
     *
     * <h2>Shutdown Process</h2> When called this will start shutdown of the record processor, and eventually shutdown
     * the worker itself.
     * <ol>
     * <li>Call to start shutdown invoked</li>
     * <li>Lease coordinator told to stop taking leases, and to drop existing leases.</li>
     * <li>Worker discovers record processors that no longer have leases.</li>
     * <li>Worker triggers shutdown with state {@link ShutdownReason#ZOMBIE}.</li>
     * <li>Once all record processors are shutdown, worker terminates owned resources.</li>
     * <li>Shutdown complete.</li>
     * </ol>
     */
    public void shutdown() {
        if (shutdown) {
            log.warn("Shutdown requested a second time.");
            return;
        }
        log.info("Worker shutdown requested.");

        // Set shutdown flag, so Worker.run can start shutdown process.
        shutdown = true;
        shutdownStartTimeMillis = System.currentTimeMillis();

        // Stop lease coordinator, so leases are not renewed or stolen from other workers.
        // Lost leases will force Worker to begin shutdown process for all shard consumers in
        // Worker.run().
        leaseCoordinator.stop();
        workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.SHUT_DOWN);
    }

    /**
     * Perform final shutdown related tasks for the worker including shutting down worker owned executor services,
     * threads, etc.
     */
    private void finalShutdown() {
        log.info("Starting worker's final shutdown.");

        if (executorService instanceof Worker.WorkerThreadPoolExecutor) {
            // This should interrupt all active record processor tasks.
            executorService.shutdownNow();
        }
        if (metricsFactory instanceof Worker.WorkerCWMetricsFactory) {
            ((CWMetricsFactory) metricsFactory).shutdown();
        }
        shutdownComplete = true;
    }

    private List<ShardInfo> getShardInfoForAssignments() {
        List<ShardInfo> assignedStreamShards = leaseCoordinator.getCurrentAssignments();
        List<ShardInfo> prioritizedShards = shardPrioritization.prioritize(assignedStreamShards);

        if ((prioritizedShards != null) && (!prioritizedShards.isEmpty())) {
            if (wlog.isInfoEnabled()) {
                StringBuilder builder = new StringBuilder();
                boolean firstItem = true;
                for (ShardInfo shardInfo : prioritizedShards) {
                    if (!firstItem) {
                        builder.append(", ");
                    }
                    builder.append(shardInfo.getShardId());
                    firstItem = false;
                }
                wlog.info("Current stream shard assignments: " + builder.toString());
            }
        } else {
            wlog.info("No activities assigned");
        }

        return prioritizedShards;
    }

    /**
     * NOTE: This method is internal/private to the Worker class. It has package access solely for testing.
     *
     * @param shardInfo
     *            Kinesis shard info
     * @param processorFactory
     *            RecordProcessor factory
     * @return ShardConsumer for the shard
     */
    ShardConsumer createOrGetShardConsumer(ShardInfo shardInfo, ProcessorFactory processorFactory) {
        ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);
        // Instantiate a new consumer if we don't have one, or the one we
        // had was from an earlier
        // lease instance (and was shutdown). Don't need to create another
        // one if the shard has been
        // completely processed (shutdown reason terminate).
        if ((consumer == null)
                || (consumer.isShutdown() && consumer.getShutdownReason().equals(ShutdownReason.ZOMBIE))) {
            consumer = buildConsumer(shardInfo, processorFactory);
            shardInfoShardConsumerMap.put(shardInfo, consumer);
            wlog.infoForce("Created new shardConsumer for : " + shardInfo);
        }
        return consumer;
    }

    private static StreamConfig createStreamConfig(@NonNull final IKinesisProxy kinesisProxy,
                                                   final int maxRecords,
                                                   final long idleTimeInMilliseconds,
                                                   final boolean shouldCallProcessRecordsEvenForEmptyRecordList,
                                                   final boolean validateSequenceNumberBeforeCheckpointing,
                                                   @NonNull final InitialPositionInStreamExtended initialPosition) {
        return new StreamConfig(kinesisProxy, maxRecords, idleTimeInMilliseconds,
                shouldCallProcessRecordsEvenForEmptyRecordList, validateSequenceNumberBeforeCheckpointing,
                initialPosition);
    }

    protected ShardConsumer buildConsumer(ShardInfo shardInfo, ProcessorFactory processorFactory) {
        return new ShardConsumer(shardInfo,
                streamConfig,
                checkpoint,
                processorFactory.createRecordProcessor(),
                leaseCoordinator.getLeaseManager(),
                parentShardPollIntervalMillis,
                cleanupLeasesUponShardCompletion,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                retryGetRecordsInSeconds,
                maxGetRecordsThreadPool,
                config);

    }

    /**
     * NOTE: This method is internal/private to the Worker class. It has package access solely for testing.
     *
     * This method relies on ShardInfo.equals() method returning true for ShardInfo objects which may have been
     * instantiated with parentShardIds in a different order (and rest of the fields being the equal). For example
     * shardInfo1.equals(shardInfo2) should return true with shardInfo1 and shardInfo2 defined as follows. ShardInfo
     * shardInfo1 = new ShardInfo(shardId1, concurrencyToken1, Arrays.asList("parent1", "parent2")); ShardInfo
     * shardInfo2 = new ShardInfo(shardId1, concurrencyToken1, Arrays.asList("parent2", "parent1"));
     */
    void cleanupShardConsumers(Set<ShardInfo> assignedShards) {
        for (ShardInfo shard : shardInfoShardConsumerMap.keySet()) {
            if (!assignedShards.contains(shard)) {
                // Shutdown the consumer since we are no longer responsible for
                // the shard.
                boolean isShutdown = shardInfoShardConsumerMap.get(shard).beginShutdown();
                if (isShutdown) {
                    shardInfoShardConsumerMap.remove(shard);
                }
            }
        }
    }

    /**
     * Logger for suppressing too much INFO logging. To avoid too much logging information Worker will output logging at
     * INFO level for a single pass through the main loop every minute. At DEBUG level it will output all INFO logs on
     * every pass.
     */
    private static class WorkerLog {

        private long reportIntervalMillis = TimeUnit.MINUTES.toMillis(1);
        private long nextReportTime = System.currentTimeMillis() + reportIntervalMillis;
        private boolean infoReporting;

        private WorkerLog() {

        }

        @SuppressWarnings("unused")
        public void debug(Object message, Throwable t) {
            log.debug("{}", message, t);
        }

        public void info(Object message) {
            if (this.isInfoEnabled()) {
                log.info("{}", message);
            }
        }

        public void infoForce(Object message) {
            log.info("{}", message);
        }

        @SuppressWarnings("unused")
        public void warn(Object message) {
            log.warn("{}", message);
        }

        @SuppressWarnings("unused")
        public void error(Object message, Throwable t) {
            log.error("{}", message, t);
        }

        private boolean isInfoEnabled() {
            return infoReporting;
        }

        private void resetInfoLogging() {
            if (infoReporting) {
                // We just logged at INFO level for a pass through worker loop
                if (log.isInfoEnabled()) {
                    infoReporting = false;
                    nextReportTime = System.currentTimeMillis() + reportIntervalMillis;
                } // else is DEBUG or TRACE so leave reporting true
            } else if (nextReportTime <= System.currentTimeMillis()) {
                infoReporting = true;
            }
        }
    }
}
