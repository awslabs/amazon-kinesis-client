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

package software.amazon.kinesis.coordinator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.base.Stopwatch;
import io.reactivex.plugins.RxJavaPlugins;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.utils.Validate;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardPrioritization;
import software.amazon.kinesis.leases.ShardSyncTask;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.DynamoDBMultiStreamLeaseSerializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.lifecycle.ShardConsumerArgument;
import software.amazon.kinesis.lifecycle.ShardConsumerShutdownNotification;
import software.amazon.kinesis.lifecycle.ShutdownNotification;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.metrics.CloudWatchMetricsFactory;
import software.amazon.kinesis.metrics.MetricsCollectingTaskDecorator;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.ShutdownNotificationAware;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalConfig;

/**
 *
 */
@Getter
@Accessors(fluent = true)
@Slf4j
public class Scheduler implements Runnable {

    private static final long NEW_STREAM_CHECK_INTERVAL_MILLIS = 1 * 60 * 1000L;

    private SchedulerLog slog = new SchedulerLog();

    private final CheckpointConfig checkpointConfig;
    private final CoordinatorConfig coordinatorConfig;
    private final LeaseManagementConfig leaseManagementConfig;
    private final LifecycleConfig lifecycleConfig;
    private final MetricsConfig metricsConfig;
    private final ProcessorConfig processorConfig;
    private final RetrievalConfig retrievalConfig;

    private final String applicationName;
    private final int maxInitializationAttempts;
    private final Checkpointer checkpoint;
    private final long shardConsumerDispatchPollIntervalMillis;
    // Backoff time when polling to check if application has finished processing
    // parent shards
    private final long parentShardPollIntervalMillis;
    private final ExecutorService executorService;
    private final DiagnosticEventFactory diagnosticEventFactory;
    private final DiagnosticEventHandler diagnosticEventHandler;
    // private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private final LeaseCoordinator leaseCoordinator;
    private final Function<StreamConfig, ShardSyncTaskManager> shardSyncTaskManagerProvider;
    private final Map<StreamConfig, ShardSyncTaskManager> streamToShardSyncTaskManagerMap = new HashMap<>();
    private final ShardPrioritization shardPrioritization;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist;
    private final GracefulShutdownCoordinator gracefulShutdownCoordinator;
    private final WorkerStateChangeListener workerStateChangeListener;
    private final MetricsFactory metricsFactory;
    private final long failoverTimeMillis;
    private final long taskBackoffTimeMillis;
    private final boolean isMultiStreamMode;
    // TODO : halo : make sure we generate streamConfig if entry not present.
    private final Map<StreamIdentifier, StreamConfig> currentStreamConfigMap;
    private MultiStreamTracker multiStreamTracker;
    private final long listShardsBackoffTimeMillis;
    private final int maxListShardsRetryAttempts;
    private final LeaseRefresher leaseRefresher;
    private final Function<StreamConfig, ShardDetector> shardDetectorProvider;
    private final boolean ignoreUnexpetedChildShards;
    private final AggregatorUtil aggregatorUtil;
    private final HierarchicalShardSyncer hierarchicalShardSyncer;
    private final long schedulerInitializationBackoffTimeMillis;

    // Holds consumers for shards the worker is currently tracking. Key is shard
    // info, value is ShardConsumer.
    private ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap = new ConcurrentHashMap<>();

    private volatile boolean shutdown;
    private volatile long shutdownStartTimeMillis;
    private volatile boolean shutdownComplete = false;

    private final Object lock = new Object();

    private Stopwatch streamSyncWatch = Stopwatch.createUnstarted();
    private boolean leasesSyncedOnAppInit = false;

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
                     @NonNull final RetrievalConfig retrievalConfig) {
        this(checkpointConfig, coordinatorConfig, leaseManagementConfig, lifecycleConfig, metricsConfig,
                processorConfig, retrievalConfig, new DiagnosticEventFactory());
    }

    /**
     * Customers do not currently have the ability to customize the DiagnosticEventFactory, but this visibility
     * is desired for testing. This constructor is only used for testing to provide a mock DiagnosticEventFactory.
     */
    @VisibleForTesting
    protected Scheduler(@NonNull final CheckpointConfig checkpointConfig,
                        @NonNull final CoordinatorConfig coordinatorConfig,
                        @NonNull final LeaseManagementConfig leaseManagementConfig,
                        @NonNull final LifecycleConfig lifecycleConfig,
                        @NonNull final MetricsConfig metricsConfig,
                        @NonNull final ProcessorConfig processorConfig,
                        @NonNull final RetrievalConfig retrievalConfig,
                        @NonNull final DiagnosticEventFactory diagnosticEventFactory) {
        this.checkpointConfig = checkpointConfig;
        this.coordinatorConfig = coordinatorConfig;
        this.leaseManagementConfig = leaseManagementConfig;
        this.lifecycleConfig = lifecycleConfig;
        this.metricsConfig = metricsConfig;
        this.processorConfig = processorConfig;
        this.retrievalConfig = retrievalConfig;

        this.applicationName = this.coordinatorConfig.applicationName();
        this.isMultiStreamMode = this.retrievalConfig.appStreamTracker().map(
                multiStreamTracker -> true, streamConfig -> false);
        this.currentStreamConfigMap = this.retrievalConfig.appStreamTracker().map(
                multiStreamTracker -> {
                    this.multiStreamTracker = multiStreamTracker;
                    return multiStreamTracker.streamConfigList().stream()
                            .collect(Collectors.toMap(sc -> sc.streamIdentifier(), sc -> sc));
                },
                streamConfig ->
                        Collections.singletonMap(streamConfig.streamIdentifier(), streamConfig));
        this.maxInitializationAttempts = this.coordinatorConfig.maxInitializationAttempts();
        this.metricsFactory = this.metricsConfig.metricsFactory();
        // Determine leaseSerializer based on availability of MultiStreamTracker.
        final LeaseSerializer leaseSerializer = isMultiStreamMode ?
                new DynamoDBMultiStreamLeaseSerializer() :
                new DynamoDBLeaseSerializer();
        this.leaseCoordinator = this.leaseManagementConfig
                .leaseManagementFactory(leaseSerializer, isMultiStreamMode)
                .createLeaseCoordinator(this.metricsFactory);
        this.leaseRefresher = this.leaseCoordinator.leaseRefresher();

        //
        // TODO: Figure out what to do with lease manage <=> checkpoint relationship
        //
        this.checkpoint = this.checkpointConfig.checkpointFactory().createCheckpointer(this.leaseCoordinator,
                this.leaseRefresher);

        //
        // TODO: Move this configuration to lifecycle
        //
        this.shardConsumerDispatchPollIntervalMillis = this.coordinatorConfig.shardConsumerDispatchPollIntervalMillis();
        this.parentShardPollIntervalMillis = this.coordinatorConfig.parentShardPollIntervalMillis();
        this.executorService = this.coordinatorConfig.coordinatorFactory().createExecutorService();
        this.diagnosticEventFactory = diagnosticEventFactory;
        this.diagnosticEventHandler = new DiagnosticEventLogger();
        // TODO : Halo : Handle case of no StreamConfig present in streamConfigList() for the supplied streamName.
        // TODO : Pass the immutable map here instead of using mst.streamConfigList()
        this.shardSyncTaskManagerProvider = streamConfig -> this.leaseManagementConfig
                .leaseManagementFactory(leaseSerializer, isMultiStreamMode)
                .createShardSyncTaskManager(this.metricsFactory, streamConfig);
        this.shardPrioritization = this.coordinatorConfig.shardPrioritization();
        this.cleanupLeasesUponShardCompletion = this.leaseManagementConfig.cleanupLeasesUponShardCompletion();
        this.skipShardSyncAtWorkerInitializationIfLeasesExist =
                this.coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist();
        if (coordinatorConfig.gracefulShutdownCoordinator() != null) {
            this.gracefulShutdownCoordinator = coordinatorConfig.gracefulShutdownCoordinator();
        } else {
            this.gracefulShutdownCoordinator = this.coordinatorConfig.coordinatorFactory()
                    .createGracefulShutdownCoordinator();
        }
        if (coordinatorConfig.workerStateChangeListener() != null) {
            this.workerStateChangeListener = coordinatorConfig.workerStateChangeListener();
        } else {
            this.workerStateChangeListener = this.coordinatorConfig.coordinatorFactory()
                    .createWorkerStateChangeListener();
        }
        this.failoverTimeMillis = this.leaseManagementConfig.failoverTimeMillis();
        this.taskBackoffTimeMillis = this.lifecycleConfig.taskBackoffTimeMillis();
//        this.retryGetRecordsInSeconds = this.retrievalConfig.retryGetRecordsInSeconds();
//        this.maxGetRecordsThreadPool = this.retrievalConfig.maxGetRecordsThreadPool();
        this.listShardsBackoffTimeMillis = this.retrievalConfig.listShardsBackoffTimeInMillis();
        this.maxListShardsRetryAttempts = this.retrievalConfig.maxListShardsRetryAttempts();
        this.shardDetectorProvider = streamConfig -> createOrGetShardSyncTaskManager(streamConfig).shardDetector();
        this.ignoreUnexpetedChildShards = this.leaseManagementConfig.ignoreUnexpectedChildShards();
        this.aggregatorUtil = this.lifecycleConfig.aggregatorUtil();
        // TODO : Halo : Check if this needs to be per stream.
        this.hierarchicalShardSyncer = leaseManagementConfig.hierarchicalShardSyncer(isMultiStreamMode);
        this.schedulerInitializationBackoffTimeMillis = this.coordinatorConfig.schedulerInitializationBackoffTimeMillis();
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
        } catch (RuntimeException e) {
            log.error("Unable to initialize after {} attempts. Shutting down.", maxInitializationAttempts, e);
            workerStateChangeListener.onAllInitializationAttemptsFailed(e);
            shutdown();
        }

        while (!shouldShutdown()) {
            runProcessLoop();
        }

        finalShutdown();
        log.info("Worker loop is complete. Exiting from worker.");
    }

    @VisibleForTesting
    void initialize() {
        synchronized (lock) {
            registerErrorHandlerForUndeliverableAsyncTaskExceptions();
            workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.INITIALIZING);
            boolean isDone = false;
            Exception lastException = null;

            for (int i = 0; (!isDone) && (i < maxInitializationAttempts); i++) {
                try {
                    log.info("Initialization attempt {}", (i + 1));
                    log.info("Initializing LeaseCoordinator");
                    leaseCoordinator.initialize();

                    TaskResult result;
                    if (!skipShardSyncAtWorkerInitializationIfLeasesExist || leaseRefresher.isLeaseTableEmpty()) {
                        // TODO: Resume the shard sync from failed stream in the next attempt, to avoid syncing
                        // TODO: for already synced streams
                        for(Map.Entry<StreamIdentifier, StreamConfig> streamConfigEntry : currentStreamConfigMap.entrySet()) {
                            final StreamIdentifier streamIdentifier = streamConfigEntry.getKey();
                            log.info("Syncing Kinesis shard info for " + streamIdentifier);
                            final StreamConfig streamConfig = streamConfigEntry.getValue();
                            ShardSyncTask shardSyncTask = new ShardSyncTask(shardDetectorProvider.apply(streamConfig),
                                    leaseRefresher, streamConfig.initialPositionInStreamExtended(),
                                    cleanupLeasesUponShardCompletion, ignoreUnexpetedChildShards, 0L,
                                    hierarchicalShardSyncer, metricsFactory);
                            result = new MetricsCollectingTaskDecorator(shardSyncTask, metricsFactory).call();
                            // Throwing the exception, to prevent further syncs for other stream.
                            if (result.getException() != null) {
                                log.error("Caught exception when sync'ing info for " + streamIdentifier, result.getException());
                                throw result.getException();
                            }
                        }
                    } else {
                        log.info("Skipping shard sync per configuration setting (and lease table is not empty)");
                    }

                    // If we reach this point, then we either skipped the lease sync or did not have any exception
                    // for any of the shard sync in the previous attempt.
                    if (!leaseCoordinator.isRunning()) {
                        log.info("Starting LeaseCoordinator");
                        leaseCoordinator.start();
                    } else {
                        log.info("LeaseCoordinator is already running. No need to start it.");
                    }
                    streamSyncWatch.start();
                    isDone = true;
                } catch (LeasingException e) {
                    log.error("Caught exception when initializing LeaseCoordinator", e);
                    lastException = e;
                } catch (Exception e) {
                    lastException = e;
                }

                if (!isDone) {
                    try {
                        Thread.sleep(schedulerInitializationBackoffTimeMillis);
                    } catch (InterruptedException e) {
                        log.debug("Sleep interrupted while initializing worker.");
                    }
                }
            }

            if (!isDone) {
                throw new RuntimeException(lastException);
            }
            workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.STARTED);
        }
    }

    @VisibleForTesting
    void runProcessLoop() {
        try {
            Set<ShardInfo> assignedShards = new HashSet<>();
            final Set<ShardInfo> completedShards = new HashSet<>();
            for (ShardInfo shardInfo : getShardInfoForAssignments()) {
                ShardConsumer shardConsumer = createOrGetShardConsumer(shardInfo,
                        processorConfig.shardRecordProcessorFactory());

                if (shardConsumer.isShutdown() && shardConsumer.shutdownReason().equals(ShutdownReason.SHARD_END)) {
                    completedShards.add(shardInfo);
                } else {
                    shardConsumer.executeLifecycle();
                }
                assignedShards.add(shardInfo);
            }

            for (ShardInfo completedShard : completedShards) {
                final StreamIdentifier streamIdentifier = getStreamIdentifier(completedShard.streamIdentifierSerOpt());
                final StreamConfig streamConfig = currentStreamConfigMap
                        .getOrDefault(streamIdentifier, getDefaultStreamConfig(streamIdentifier));
                if (createOrGetShardSyncTaskManager(streamConfig).syncShardAndLeaseInfo()) {
                    log.info("{} : Found completed shard, initiated new ShardSyncTak for {} ",
                            streamIdentifier.serialize(), completedShard.toString());
                }
            }

            // clean up shard consumers for unassigned shards
            cleanupShardConsumers(assignedShards);

            // check for new streams and sync with the scheduler state
            checkAndSyncStreamShardsAndLeases();

            logExecutorState();
            slog.info("Sleeping ...");
            Thread.sleep(shardConsumerDispatchPollIntervalMillis);
        } catch (Exception e) {
            log.error("Worker.run caught exception, sleeping for {} milli seconds!",
                    String.valueOf(shardConsumerDispatchPollIntervalMillis), e);
            try {
                Thread.sleep(shardConsumerDispatchPollIntervalMillis);
            } catch (InterruptedException ex) {
                log.info("Worker: sleep interrupted after catching exception ", ex);
            }
        }
        slog.resetInfoLogging();
    }


    /**
     * Note: This method has package level access solely for testing purposes.
     * Sync all streams method.
     * @return streams that are being synced by this worker
     */
    @VisibleForTesting
    Set<StreamIdentifier> checkAndSyncStreamShardsAndLeases()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final Set<StreamIdentifier> streamsSynced = new HashSet<>();

        if (shouldSyncStreamsNow()) {
            final Map<StreamIdentifier, StreamConfig> newStreamConfigMap = new HashMap<>();
            // Making an immutable copy
            newStreamConfigMap.putAll(multiStreamTracker.streamConfigList().stream()
                    .collect(Collectors.toMap(sc -> sc.streamIdentifier(), sc -> sc)));

            // This is done to ensure that we clean up the stale streams lingering in the lease table.
            syncStreamsFromLeaseTableOnAppInit();

            for (StreamIdentifier streamIdentifier : newStreamConfigMap.keySet()) {
                if (!currentStreamConfigMap.containsKey(streamIdentifier)) {
                    log.info("Found new stream to process: " + streamIdentifier + ". Syncing shards of that stream.");
                    ShardSyncTaskManager shardSyncTaskManager = createOrGetShardSyncTaskManager(newStreamConfigMap.get(streamIdentifier));
                    shardSyncTaskManager.syncShardAndLeaseInfo();
                    currentStreamConfigMap.put(streamIdentifier, newStreamConfigMap.get(streamIdentifier));
                    streamsSynced.add(streamIdentifier);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(streamIdentifier + " is already being processed - skipping shard sync.");
                    }
                }
            }

            // TODO: Remove assumption that each Worker gets the full list of streams
            Iterator<StreamIdentifier> currentStreamConfigIter = currentStreamConfigMap.keySet().iterator();
            while (currentStreamConfigIter.hasNext()) {
                StreamIdentifier streamIdentifier = currentStreamConfigIter.next();
                if (!newStreamConfigMap.containsKey(streamIdentifier)) {
                    log.info("Found old/deleted stream: " + streamIdentifier + ". Syncing shards of that stream.");
                    ShardSyncTaskManager shardSyncTaskManager = createOrGetShardSyncTaskManager(currentStreamConfigMap.get(streamIdentifier));
                    shardSyncTaskManager.syncShardAndLeaseInfo();
                    currentStreamConfigIter.remove();
                    streamsSynced.add(streamIdentifier);
                }
            }
            streamSyncWatch.reset().start();
        }
        return streamsSynced;
    }

    @VisibleForTesting
    boolean shouldSyncStreamsNow() {
        return isMultiStreamMode && (streamSyncWatch.elapsed(TimeUnit.MILLISECONDS) > NEW_STREAM_CHECK_INTERVAL_MILLIS);
    }

    private void syncStreamsFromLeaseTableOnAppInit()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        if (!leasesSyncedOnAppInit && isMultiStreamMode) {
            final Set<StreamIdentifier> streamIdentifiers = leaseCoordinator.leaseRefresher().listLeases().stream()
                    .map(lease -> StreamIdentifier.multiStreamInstance(((MultiStreamLease) lease).streamIdentifier()))
                    .collect(Collectors.toSet());
            for (StreamIdentifier streamIdentifier : streamIdentifiers) {
                if (!currentStreamConfigMap.containsKey(streamIdentifier)) {
                    currentStreamConfigMap.put(streamIdentifier, getDefaultStreamConfig(streamIdentifier));
                }
            }
            leasesSyncedOnAppInit = true;
        }
    }

    // When a stream is no longer needed to be tracked, return a default StreamConfig with LATEST for faster shard end.
    private StreamConfig getDefaultStreamConfig(StreamIdentifier streamIdentifier) {
        return new StreamConfig(streamIdentifier, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));
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
     * Requests a graceful shutdown of the worker, notifying record processors, that implement
     * {@link ShutdownNotificationAware}, of the impending shutdown. This gives the record processor a final chance to
     * checkpoint.
     *
     * This will only create a single shutdown future. Additional attempts to start a graceful shutdown will return the
     * previous future.
     *
     * <b>It's possible that a record processor won't be notify before being shutdown. This can occur if the lease is
     * lost after requesting shutdown, but before the notification is dispatched.</b>
     *
     * <h2>Requested Shutdown Process</h2> When a shutdown process is requested it operates slightly differently to
     * allow the record processors a chance to checkpoint a final time.
     * <ol>
     * <li>Call to request shutdown invoked.</li>
     * <li>Worker stops attempting to acquire new leases</li>
     * <li>Record Processor Shutdown Begins
     * <ol>
     * <li>Record processor is notified of the impending shutdown, and given a final chance to checkpoint</li>
     * <li>The lease for the record processor is then dropped.</li>
     * <li>The record processor enters into an idle state waiting for the worker to complete final termination</li>
     * <li>The worker will detect a record processor that has lost it's lease, and will terminate the record processor
     * with {@link ShutdownReason#LEASE_LOST}</li>
     * </ol>
     * </li>
     * <li>The worker will shutdown all record processors.</li>
     * <li>Once all record processors have been terminated, the worker will terminate all owned resources.</li>
     * <li>Once the worker shutdown is complete, the returned future is completed.</li>
     * </ol>
     *
     * @return a future that will be set once the shutdown has completed. True indicates that the graceful shutdown
     *         completed successfully. A false value indicates that a non-exception case caused the shutdown process to
     *         terminate early.
     */
    public Future<Boolean> startGracefulShutdown() {
        synchronized (this) {
            if (gracefulShutdownFuture == null) {
                gracefulShutdownFuture = gracefulShutdownCoordinator
                        .startGracefulShutdown(createGracefulShutdownCallable());
            }
        }
        return gracefulShutdownFuture;
    }

    /**
     * Creates a callable that will execute the graceful shutdown process. This callable can be used to execute graceful
     * shutdowns in your own executor, or execute the shutdown synchronously.
     *
     * @return a callable that run the graceful shutdown process. This may return a callable that return true if the
     *         graceful shutdown has already been completed.
     * @throws IllegalStateException
     *             thrown by the callable if another callable has already started the shutdown process.
     */
    public Callable<Boolean> createGracefulShutdownCallable() {
        if (shutdownComplete()) {
            return () -> true;
        }
        Callable<GracefulShutdownContext> startShutdown = createWorkerShutdownCallable();
        return gracefulShutdownCoordinator.createGracefulShutdownCallable(startShutdown);
    }

    public boolean hasGracefulShutdownStarted() {
        return gracefuleShutdownStarted;
    }

    @VisibleForTesting
    Callable<GracefulShutdownContext> createWorkerShutdownCallable() {
        return () -> {
            synchronized (this) {
                if (this.gracefuleShutdownStarted) {
                    throw new IllegalStateException("Requested shutdown has already been started");
                }
                this.gracefuleShutdownStarted = true;
            }
            //
            // Stop accepting new leases. Once we do this we can be sure that
            // no more leases will be acquired.
            //
            leaseCoordinator.stopLeaseTaker();

            Collection<Lease> leases = leaseCoordinator.getAssignments();
            if (leases == null || leases.isEmpty()) {
                //
                // If there are no leases notification is already completed, but we still need to shutdown the worker.
                //
                this.shutdown();
                return GracefulShutdownContext.SHUTDOWN_ALREADY_COMPLETED;
            }
            CountDownLatch shutdownCompleteLatch = new CountDownLatch(leases.size());
            CountDownLatch notificationCompleteLatch = new CountDownLatch(leases.size());
            for (Lease lease : leases) {
                ShutdownNotification shutdownNotification = new ShardConsumerShutdownNotification(leaseCoordinator,
                        lease, notificationCompleteLatch, shutdownCompleteLatch);
                ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
                ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);
                if (consumer != null) {
                    consumer.gracefulShutdown(shutdownNotification);
                } else {
                    //
                    // There is a race condition between retrieving the current assignments, and creating the
                    // notification. If the a lease is lost in between these two points, we explicitly decrement the
                    // notification latches to clear the shutdown.
                    //
                    notificationCompleteLatch.countDown();
                    shutdownCompleteLatch.countDown();
                }
            }
            return new GracefulShutdownContext(shutdownCompleteLatch, notificationCompleteLatch, this);
        };
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
     * <li>Worker triggers shutdown with state {@link ShutdownReason#LEASE_LOST}.</li>
     * <li>Once all record processors are shutdown, worker terminates owned resources.</li>
     * <li>Shutdown complete.</li>
     * </ol>
     */
    public void shutdown() {
        synchronized (lock) {
            if (shutdown) {
                log.warn("Shutdown requested a second time.");
                return;
            }
            workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.SHUT_DOWN_STARTED);
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
    }

    /**
     * Perform final shutdown related tasks for the worker including shutting down worker owned executor services,
     * threads, etc.
     */
    private void finalShutdown() {
        log.info("Starting worker's final shutdown.");

        if (executorService instanceof SchedulerCoordinatorFactory.SchedulerThreadPoolExecutor) {
            // This should interrupt all active record processor tasks.
            executorService.shutdownNow();
        }
        if (metricsFactory instanceof CloudWatchMetricsFactory) {
            ((CloudWatchMetricsFactory) metricsFactory).shutdown();
        }
        shutdownComplete = true;
    }

    private List<ShardInfo> getShardInfoForAssignments() {
        List<ShardInfo> assignedStreamShards = leaseCoordinator.getCurrentAssignments();
        List<ShardInfo> prioritizedShards = shardPrioritization.prioritize(assignedStreamShards);

        if ((prioritizedShards != null) && (!prioritizedShards.isEmpty())) {
            if (slog.isInfoEnabled()) {
                StringBuilder builder = new StringBuilder();
                boolean firstItem = true;
                for (ShardInfo shardInfo : prioritizedShards) {
                    if (!firstItem) {
                        builder.append(", ");
                    }
                    builder.append(shardInfo.streamIdentifierSerOpt().map(s -> s + ":" + shardInfo.shardId())
                            .orElse(shardInfo.shardId()));
                    firstItem = false;
                }
                slog.info("Current stream shard assignments: " + builder.toString());
            }
        } else {
            slog.info("No activities assigned");
        }

        return prioritizedShards;
    }

    /**
     * NOTE: This method is internal/private to the Worker class. It has package access solely for testing.
     *
     * @param shardInfo
     *            Kinesis shard info
     * @return ShardConsumer for the shard
     */
    ShardConsumer createOrGetShardConsumer(@NonNull final ShardInfo shardInfo,
            @NonNull final ShardRecordProcessorFactory shardRecordProcessorFactory) {
        ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);
        // Instantiate a new consumer if we don't have one, or the one we
        // had was from an earlier
        // lease instance (and was shutdown). Don't need to create another
        // one if the shard has been
        // completely processed (shutdown reason terminate).
        if ((consumer == null)
                || (consumer.isShutdown() && consumer.shutdownReason().equals(ShutdownReason.LEASE_LOST))) {
            consumer = buildConsumer(shardInfo, shardRecordProcessorFactory);
            shardInfoShardConsumerMap.put(shardInfo, consumer);
            slog.infoForce("Created new shardConsumer for : " + shardInfo);
        }
        return consumer;
    }

    private ShardSyncTaskManager createOrGetShardSyncTaskManager(StreamConfig streamConfig) {
        return streamToShardSyncTaskManagerMap.computeIfAbsent(streamConfig, s -> shardSyncTaskManagerProvider.apply(s));
    }

    protected ShardConsumer buildConsumer(@NonNull final ShardInfo shardInfo,
            @NonNull final ShardRecordProcessorFactory shardRecordProcessorFactory) {
        RecordsPublisher cache = retrievalConfig.retrievalFactory().createGetRecordsCache(shardInfo, metricsFactory);
        ShardRecordProcessorCheckpointer checkpointer = coordinatorConfig.coordinatorFactory().createRecordProcessorCheckpointer(shardInfo,
                        checkpoint);
        // The only case where streamName is not available will be when multistreamtracker not set. In this case,
        // get the default stream name for the single stream application.
        final StreamIdentifier streamIdentifier = getStreamIdentifier(shardInfo.streamIdentifierSerOpt());
        // Irrespective of single stream app or multi stream app, streamConfig should always be available.
        // If we have a shardInfo, that is not present in currentStreamConfigMap for whatever reason, then return default stream config
        // to gracefully complete the reading.
        final StreamConfig streamConfig = currentStreamConfigMap.getOrDefault(streamIdentifier, getDefaultStreamConfig(streamIdentifier));
        Validate.notNull(streamConfig, "StreamConfig should not be null");
        ShardConsumerArgument argument = new ShardConsumerArgument(shardInfo,
                streamConfig.streamIdentifier(),
                leaseCoordinator,
                executorService,
                cache,
                shardRecordProcessorFactory.shardRecordProcessor(streamIdentifier),
                checkpoint,
                checkpointer,
                parentShardPollIntervalMillis,
                taskBackoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts,
                processorConfig.callProcessRecordsEvenForEmptyRecordList(),
                shardConsumerDispatchPollIntervalMillis,
                streamConfig.initialPositionInStreamExtended(),
                cleanupLeasesUponShardCompletion,
                ignoreUnexpetedChildShards,
                shardDetectorProvider.apply(streamConfig),
                aggregatorUtil,
                hierarchicalShardSyncer,
                metricsFactory);
        return new ShardConsumer(cache, executorService, shardInfo, lifecycleConfig.logWarningForTaskAfterMillis(),
                argument, lifecycleConfig.taskExecutionListener(),lifecycleConfig.readTimeoutsToIgnoreBeforeWarning());
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
                ShardConsumer consumer = shardInfoShardConsumerMap.get(shard);
                if (consumer.leaseLost()) {
                    shardInfoShardConsumerMap.remove(shard);
                    log.debug("Removed consumer for {} as lease has been lost",
                            shard.streamIdentifierSerOpt().map(s -> s + ":" + shard.shardId()).orElse(shard.shardId()));
                } else {
                    consumer.executeLifecycle();
                }
            }
        }
    }

    /**
     * Exceptions in the RxJava layer can fail silently unless an error handler is set to propagate these exceptions
     * back to the KCL, as is done below.
     */
    private void registerErrorHandlerForUndeliverableAsyncTaskExceptions() {
        RxJavaPlugins.setErrorHandler(t -> {
            ExecutorStateEvent executorStateEvent = diagnosticEventFactory.executorStateEvent(executorService,
                    leaseCoordinator);
            RejectedTaskEvent rejectedTaskEvent = diagnosticEventFactory.rejectedTaskEvent(executorStateEvent, t);
            rejectedTaskEvent.accept(diagnosticEventHandler);
        });
    }

    private void logExecutorState() {
        ExecutorStateEvent executorStateEvent = diagnosticEventFactory.executorStateEvent(executorService,
                leaseCoordinator);
        executorStateEvent.accept(diagnosticEventHandler);
    }

    private StreamIdentifier getStreamIdentifier(Optional<String> streamIdentifierString) {
        final StreamIdentifier streamIdentifier;
        if(streamIdentifierString.isPresent()) {
            streamIdentifier = StreamIdentifier.multiStreamInstance(streamIdentifierString.get());
        } else {
            Validate.isTrue(!isMultiStreamMode, "Should not be in MultiStream Mode");
            streamIdentifier = this.currentStreamConfigMap.values().iterator().next().streamIdentifier();
        }
        Validate.notNull(streamIdentifier, "Stream identifier should not be empty");
        return streamIdentifier;
    }

    /**
     * Logger for suppressing too much INFO logging. To avoid too much logging information Worker will output logging at
     * INFO level for a single pass through the main loop every minute. At DEBUG level it will output all INFO logs on
     * every pass.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class SchedulerLog {

        private long reportIntervalMillis = TimeUnit.MINUTES.toMillis(1);
        private long nextReportTime = System.currentTimeMillis() + reportIntervalMillis;
        private boolean infoReporting;

        void info(Object message) {
            if (this.isInfoEnabled()) {
                log.info("{}", message);
            }
        }

        void infoForce(Object message) {
            log.info("{}", message);
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

    @Deprecated
    public Future<Void> requestShutdown() {
        return null;
    }
}
