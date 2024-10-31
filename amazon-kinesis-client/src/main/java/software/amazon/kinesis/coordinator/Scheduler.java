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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.Validate;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.assignment.LeaseAssignmentManager;
import software.amazon.kinesis.coordinator.migration.MigrationStateMachine;
import software.amazon.kinesis.coordinator.migration.MigrationStateMachineImpl;
import software.amazon.kinesis.leader.DynamoDBLockBasedLeaderDecider;
import software.amazon.kinesis.leader.MigrationAdaptiveLeaderDecider;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig;
import software.amazon.kinesis.leases.LeaseManagementFactory;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardPrioritization;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.DynamoDBMultiStreamLeaseSerializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.lifecycle.ShardConsumerArgument;
import software.amazon.kinesis.lifecycle.ShardConsumerShutdownNotification;
import software.amazon.kinesis.lifecycle.ShutdownNotification;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.metrics.CloudWatchMetricsFactory;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.schemaregistry.SchemaRegistryDecoder;
import software.amazon.kinesis.worker.WorkerMetricsSelector;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsManager;

import static software.amazon.kinesis.common.ArnUtil.constructStreamArn;
import static software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy.StreamsLeasesDeletionType;
import static software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy.StreamsLeasesDeletionType.FORMER_STREAMS_AUTO_DETECTION_DEFERRED_DELETION;

/**
 *
 */
@Getter(AccessLevel.PRIVATE)
@Accessors(fluent = true)
@Slf4j
@KinesisClientInternalApi
public class Scheduler implements Runnable {

    private static final int PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT = 1;

    private static final long LEASE_TABLE_CHECK_FREQUENCY_MILLIS = 3 * 1000L;
    private static final long MIN_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS = 1000L;
    private static final long MAX_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS = 30 * 1000L;
    private static final long NEW_STREAM_CHECK_INTERVAL_MILLIS = 60_000L;
    private static final boolean SHOULD_DO_LEASE_SYNC_FOR_OLD_STREAMS = false;
    private static final String MULTI_STREAM_TRACKER = "MultiStreamTracker";
    private static final String ACTIVE_STREAMS_COUNT = "ActiveStreams.Count";
    private static final String PENDING_STREAMS_DELETION_COUNT = "StreamsPendingDeletion.Count";
    private static final String DELETED_STREAMS_COUNT = "DeletedStreams.Count";
    private static final String NON_EXISTING_STREAM_DELETE_COUNT = "NonExistingStreamDelete.Count";

    private final SchedulerLog slog = new SchedulerLog();

    private final CheckpointConfig checkpointConfig;
    private final CoordinatorConfig coordinatorConfig;
    private final LeaseManagementConfig leaseManagementConfig;
    private final LifecycleConfig lifecycleConfig;
    private final MetricsConfig metricsConfig;
    private final ProcessorConfig processorConfig;
    private final RetrievalConfig retrievalConfig;

    @Getter(AccessLevel.PACKAGE)
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
    private final LeaseCoordinator leaseCoordinator;
    private final Function<StreamConfig, ShardSyncTaskManager> shardSyncTaskManagerProvider;
    private final Map<StreamConfig, ShardSyncTaskManager> streamToShardSyncTaskManagerMap = new ConcurrentHashMap<>();
    private final PeriodicShardSyncManager leaderElectedPeriodicShardSyncManager;
    private final ShardPrioritization shardPrioritization;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist;
    private final GracefulShutdownCoordinator gracefulShutdownCoordinator;
    private final WorkerStateChangeListener workerStateChangeListener;
    private final MetricsFactory metricsFactory;
    private final long failoverTimeMillis;
    private final long taskBackoffTimeMillis;
    private final boolean isMultiStreamMode;

    @Getter(AccessLevel.PACKAGE)
    private final Map<StreamIdentifier, StreamConfig> currentStreamConfigMap = new StreamConfigMap();

    private final StreamTracker streamTracker;
    private final FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy;
    private final long listShardsBackoffTimeMillis;
    private final int maxListShardsRetryAttempts;
    private final LeaseRefresher leaseRefresher;
    private final Function<StreamConfig, ShardDetector> shardDetectorProvider;
    private final boolean ignoreUnexpetedChildShards;
    private final AggregatorUtil aggregatorUtil;
    private final Function<StreamConfig, HierarchicalShardSyncer> hierarchicalShardSyncerProvider;
    private final long schedulerInitializationBackoffTimeMillis;
    private LeaderDecider leaderDecider;

    @Getter(AccessLevel.PACKAGE)
    private final Map<StreamIdentifier, Instant> staleStreamDeletionMap = new HashMap<>();

    private final LeaseCleanupManager leaseCleanupManager;
    private final SchemaRegistryDecoder schemaRegistryDecoder;

    @Getter(AccessLevel.PACKAGE)
    private final DeletedStreamListProvider deletedStreamListProvider;

    private final MigrationStateMachine migrationStateMachine;
    private final DynamicMigrationComponentsInitializer migrationComponentsInitializer;
    private final MigrationAdaptiveLeaseAssignmentModeProvider leaseAssignmentModeProvider;

    // Holds consumers for shards the worker is currently tracking. Key is shard
    // info, value is ShardConsumer.
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap = new ConcurrentHashMap<>();

    private volatile boolean shutdown;
    private volatile long shutdownStartTimeMillis;

    @Getter(AccessLevel.PACKAGE)
    private volatile boolean shutdownComplete = false;

    private final Object lock = new Object();

    private final Stopwatch streamSyncWatch = Stopwatch.createUnstarted();

    private boolean leasesSyncedOnAppInit = false;
    private final AtomicBoolean leaderSynced = new AtomicBoolean(false);

    /**
     * Used to ensure that only one requestedShutdown is in progress at a time.
     */
    private CompletableFuture<Boolean> gracefulShutdownFuture;

    /**
     * CountDownLatch used by the GracefulShutdownCoordinator. Reaching zero means that
     * the scheduler's finalShutdown() call has completed.
     */
    private final CountDownLatch finalShutdownLatch = new CountDownLatch(1);

    @VisibleForTesting
    protected boolean gracefuleShutdownStarted = false;

    public Scheduler(
            @NonNull final CheckpointConfig checkpointConfig,
            @NonNull final CoordinatorConfig coordinatorConfig,
            @NonNull final LeaseManagementConfig leaseManagementConfig,
            @NonNull final LifecycleConfig lifecycleConfig,
            @NonNull final MetricsConfig metricsConfig,
            @NonNull final ProcessorConfig processorConfig,
            @NonNull final RetrievalConfig retrievalConfig) {
        this(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig,
                new DiagnosticEventFactory());
    }

    /**
     * Customers do not currently have the ability to customize the DiagnosticEventFactory, but this visibility
     * is desired for testing. This constructor is only used for testing to provide a mock DiagnosticEventFactory.
     */
    @VisibleForTesting
    protected Scheduler(
            @NonNull final CheckpointConfig checkpointConfig,
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
        this.streamTracker = retrievalConfig.streamTracker();
        this.isMultiStreamMode = streamTracker.isMultiStream();
        this.formerStreamsLeasesDeletionStrategy = streamTracker.formerStreamsLeasesDeletionStrategy();
        streamTracker.streamConfigList().forEach(sc -> currentStreamConfigMap.put(sc.streamIdentifier(), sc));
        log.info("Initial state: {}", currentStreamConfigMap.values());

        this.maxInitializationAttempts = this.coordinatorConfig.maxInitializationAttempts();
        this.metricsFactory = this.metricsConfig.metricsFactory();
        // Determine leaseSerializer based on availability of MultiStreamTracker.
        final LeaseSerializer leaseSerializer =
                isMultiStreamMode ? new DynamoDBMultiStreamLeaseSerializer() : new DynamoDBLeaseSerializer();

        final LeaseManagementFactory leaseManagementFactory =
                this.leaseManagementConfig.leaseManagementFactory(leaseSerializer, isMultiStreamMode);
        this.leaseCoordinator =
                leaseManagementFactory.createLeaseCoordinator(this.metricsFactory, shardInfoShardConsumerMap);
        this.leaseRefresher = this.leaseCoordinator.leaseRefresher();

        final CoordinatorStateDAO coordinatorStateDAO = new CoordinatorStateDAO(
                leaseManagementConfig.dynamoDBClient(), coordinatorConfig().coordinatorStateConfig());
        this.leaseAssignmentModeProvider = new MigrationAdaptiveLeaseAssignmentModeProvider();
        this.migrationComponentsInitializer = createDynamicMigrationComponentsInitializer(coordinatorStateDAO);
        this.migrationStateMachine = new MigrationStateMachineImpl(
                metricsFactory,
                System::currentTimeMillis,
                coordinatorStateDAO,
                Executors.newScheduledThreadPool(
                        2,
                        new ThreadFactoryBuilder()
                                .setNameFormat("MigrationStateMachine-%04d")
                                .build()),
                coordinatorConfig.clientVersionConfig(),
                new Random(),
                this.migrationComponentsInitializer,
                leaseManagementConfig.workerIdentifier(),
                Duration.ofMinutes(10).getSeconds());

        //
        // TODO: Figure out what to do with lease manage <=> checkpoint relationship
        //
        this.checkpoint = this.checkpointConfig
                .checkpointFactory()
                .createCheckpointer(this.leaseCoordinator, this.leaseRefresher);

        //
        // TODO: Move this configuration to lifecycle
        //
        this.shardConsumerDispatchPollIntervalMillis = this.coordinatorConfig.shardConsumerDispatchPollIntervalMillis();
        this.parentShardPollIntervalMillis = this.coordinatorConfig.parentShardPollIntervalMillis();
        this.executorService = this.coordinatorConfig.coordinatorFactory().createExecutorService();
        this.diagnosticEventFactory = diagnosticEventFactory;
        this.diagnosticEventHandler = new DiagnosticEventLogger();
        this.deletedStreamListProvider = new DeletedStreamListProvider();
        this.shardSyncTaskManagerProvider = streamConfig -> leaseManagementFactory.createShardSyncTaskManager(
                this.metricsFactory, streamConfig, this.deletedStreamListProvider);
        this.shardPrioritization = this.coordinatorConfig.shardPrioritization();
        this.cleanupLeasesUponShardCompletion = this.leaseManagementConfig.cleanupLeasesUponShardCompletion();
        this.skipShardSyncAtWorkerInitializationIfLeasesExist =
                this.coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist();
        if (coordinatorConfig.gracefulShutdownCoordinator() != null) {
            this.gracefulShutdownCoordinator = coordinatorConfig.gracefulShutdownCoordinator();
        } else {
            this.gracefulShutdownCoordinator =
                    this.coordinatorConfig.coordinatorFactory().createGracefulShutdownCoordinator();
        }
        if (coordinatorConfig.workerStateChangeListener() != null) {
            this.workerStateChangeListener = coordinatorConfig.workerStateChangeListener();
        } else {
            this.workerStateChangeListener =
                    this.coordinatorConfig.coordinatorFactory().createWorkerStateChangeListener();
        }
        this.failoverTimeMillis = this.leaseManagementConfig.failoverTimeMillis();
        this.taskBackoffTimeMillis = this.lifecycleConfig.taskBackoffTimeMillis();
        this.listShardsBackoffTimeMillis = this.retrievalConfig.listShardsBackoffTimeInMillis();
        this.maxListShardsRetryAttempts = this.retrievalConfig.maxListShardsRetryAttempts();
        this.shardDetectorProvider =
                streamConfig -> createOrGetShardSyncTaskManager(streamConfig).shardDetector();
        this.ignoreUnexpetedChildShards = this.leaseManagementConfig.ignoreUnexpectedChildShards();
        this.aggregatorUtil = this.lifecycleConfig.aggregatorUtil();
        this.hierarchicalShardSyncerProvider =
                streamConfig -> createOrGetShardSyncTaskManager(streamConfig).hierarchicalShardSyncer();
        this.schedulerInitializationBackoffTimeMillis =
                this.coordinatorConfig.schedulerInitializationBackoffTimeMillis();
        this.leaderElectedPeriodicShardSyncManager = new PeriodicShardSyncManager(
                leaseManagementConfig.workerIdentifier(),
                leaseRefresher,
                currentStreamConfigMap,
                shardSyncTaskManagerProvider,
                streamToShardSyncTaskManagerMap,
                isMultiStreamMode,
                metricsFactory,
                leaseManagementConfig.leasesRecoveryAuditorExecutionFrequencyMillis(),
                leaseManagementConfig.leasesRecoveryAuditorInconsistencyConfidenceThreshold(),
                leaderSynced);
        this.leaseCleanupManager = leaseManagementFactory.createLeaseCleanupManager(metricsFactory);
        this.schemaRegistryDecoder = this.retrievalConfig.glueSchemaRegistryDeserializer() == null
                ? null
                : new SchemaRegistryDecoder(this.retrievalConfig.glueSchemaRegistryDeserializer());
    }

    /**
     * Depends on LeaseCoordinator and LeaseRefresher to be created first
     */
    private DynamicMigrationComponentsInitializer createDynamicMigrationComponentsInitializer(
            final CoordinatorStateDAO coordinatorStateDAO) {
        selectWorkerMetricsIfAvailable(leaseManagementConfig.workerUtilizationAwareAssignmentConfig());

        final WorkerMetricStatsManager workerMetricsManager = new WorkerMetricStatsManager(
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig().noOfPersistedMetricsPerWorkerMetrics(),
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig().workerMetricList(),
                metricsFactory,
                leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .inMemoryWorkerMetricsCaptureFrequencyMillis());

        final WorkerMetricStatsDAO workerMetricsDAO = new WorkerMetricStatsDAO(
                leaseManagementConfig.dynamoDBClient(),
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig().workerMetricsTableConfig(),
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig().workerMetricsReporterFreqInMillis());

        return DynamicMigrationComponentsInitializer.builder()
                .metricsFactory(metricsFactory)
                .leaseRefresher(leaseRefresher)
                .coordinatorStateDAO(coordinatorStateDAO)
                .workerMetricsThreadPool(Executors.newScheduledThreadPool(
                        1,
                        new ThreadFactoryBuilder()
                                .setNameFormat("worker-metrics-reporter")
                                .build()))
                .workerMetricsDAO(workerMetricsDAO)
                .workerMetricsManager(workerMetricsManager)
                .lamThreadPool(Executors.newScheduledThreadPool(
                        1,
                        new ThreadFactoryBuilder().setNameFormat("lam-thread").build()))
                .lamCreator((lamThreadPool, leaderDecider) -> new LeaseAssignmentManager(
                        leaseRefresher,
                        workerMetricsDAO,
                        leaderDecider,
                        leaseManagementConfig.workerUtilizationAwareAssignmentConfig(),
                        leaseCoordinator.workerIdentifier(),
                        leaseManagementConfig.failoverTimeMillis(),
                        metricsFactory,
                        lamThreadPool,
                        System::nanoTime,
                        leaseManagementConfig.maxLeasesForWorker(),
                        leaseManagementConfig.gracefulLeaseHandoffConfig()))
                .adaptiveLeaderDeciderCreator(() -> new MigrationAdaptiveLeaderDecider(metricsFactory))
                .deterministicLeaderDeciderCreator(() -> new DeterministicShuffleShardSyncLeaderDecider(
                        leaseRefresher, Executors.newSingleThreadScheduledExecutor(), 1, metricsFactory))
                .ddbLockBasedLeaderDeciderCreator(() -> DynamoDBLockBasedLeaderDecider.create(
                        coordinatorStateDAO, leaseCoordinator.workerIdentifier(), metricsFactory))
                .workerIdentifier(leaseCoordinator.workerIdentifier())
                .workerUtilizationAwareAssignmentConfig(leaseManagementConfig.workerUtilizationAwareAssignmentConfig())
                .leaseAssignmentModeProvider(leaseAssignmentModeProvider)
                .build();
    }

    /**
     * Start consuming data from the stream, and pass it to the application record processors.
     */
    @Override
    public void run() {
        if (shutdown) {
            return;
        }

        final MetricsScope metricsScope =
                MetricsUtil.createMetricsWithOperation(metricsFactory, "Scheduler:Initialize");
        boolean success = false;
        try {
            initialize();
            success = true;
            log.info("Initialization complete. Starting worker loop.");
        } catch (RuntimeException e) {
            log.error("Unable to initialize after {} attempts. Shutting down.", maxInitializationAttempts, e);
            workerStateChangeListener.onAllInitializationAttemptsFailed(e);
            shutdown();
        } finally {
            MetricsUtil.addSuccess(metricsScope, "Initialize", success, MetricsLevel.SUMMARY);
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
                    log.info("Initializing LeaseCoordinator attempt {}", (i + 1));
                    leaseCoordinator.initialize();
                    if (!skipShardSyncAtWorkerInitializationIfLeasesExist || leaseRefresher.isLeaseTableEmpty()) {
                        if (shouldInitiateLeaseSync()) {
                            log.info(
                                    "Worker {} is initiating the lease sync.",
                                    leaseManagementConfig.workerIdentifier());
                            leaderElectedPeriodicShardSyncManager.syncShardsOnce();
                        }
                    } else {
                        log.info("Skipping shard sync per configuration setting (and lease table is not empty)");
                    }

                    // Initialize the state machine after lease table has been initialized
                    // Migration state machine creates and waits for GSI if necessary,
                    // it must be initialized before starting leaseCoordinator, which runs LeaseDiscoverer
                    // and that requires GSI to be present and active. (migrationStateMachine.initialize is idempotent)
                    migrationStateMachine.initialize();
                    leaderDecider = migrationComponentsInitializer.leaderDecider();

                    leaseCleanupManager.start();

                    // If we reach this point, then we either skipped the lease sync or did not have any exception
                    // for any of the shard sync in the previous attempt.

                    if (!leaseCoordinator.isRunning()) {
                        log.info("Starting LeaseCoordinator");
                        leaseCoordinator.start(leaseAssignmentModeProvider);
                    } else {
                        log.info("LeaseCoordinator is already running. No need to start it.");
                    }
                    log.info("Scheduling periodicShardSync");
                    leaderElectedPeriodicShardSyncManager.start(leaderDecider);
                    streamSyncWatch.start();
                    isDone = true;
                } catch (final Exception e) {
                    log.error("Caught exception when initializing LeaseCoordinator", e);
                    lastException = e;
                }

                if (!isDone) {
                    try {
                        Thread.sleep(schedulerInitializationBackoffTimeMillis);
                        leaderElectedPeriodicShardSyncManager.stop();
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
    boolean shouldInitiateLeaseSync()
            throws InterruptedException, DependencyException, ProvisionedThroughputException, InvalidStateException {
        long waitTime = ThreadLocalRandom.current()
                .nextLong(MIN_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS, MAX_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS);
        long waitUntil = System.currentTimeMillis() + waitTime;

        boolean shouldInitiateLeaseSync = true;
        while (System.currentTimeMillis() < waitUntil
                && (shouldInitiateLeaseSync = leaseRefresher.isLeaseTableEmpty())) {
            // check every 3 seconds if lease table is still empty,
            // to minimize contention between all workers bootstrapping at the same time
            log.info("Lease table is still empty. Checking again in {} ms", LEASE_TABLE_CHECK_FREQUENCY_MILLIS);
            Thread.sleep(LEASE_TABLE_CHECK_FREQUENCY_MILLIS);
        }
        return shouldInitiateLeaseSync;
    }

    @VisibleForTesting
    void runProcessLoop() {
        try {
            Set<ShardInfo> assignedShards = new HashSet<>();
            for (ShardInfo shardInfo : getShardInfoForAssignments()) {
                ShardConsumer shardConsumer = createOrGetShardConsumer(
                        shardInfo, processorConfig.shardRecordProcessorFactory(), leaseCleanupManager);

                shardConsumer.executeLifecycle();
                assignedShards.add(shardInfo);
            }

            // clean up shard consumers for unassigned shards
            cleanupShardConsumers(assignedShards);

            // check for new streams and sync with the scheduler state
            if (isLeader()) {
                checkAndSyncStreamShardsAndLeases();
                leaderSynced.set(true);
            } else {
                leaderSynced.set(false);
            }

            logExecutorState();
            slog.info("Sleeping ...");
            Thread.sleep(shardConsumerDispatchPollIntervalMillis);
        } catch (Exception e) {
            log.error(
                    "Worker.run caught exception, sleeping for {} milli seconds!",
                    shardConsumerDispatchPollIntervalMillis,
                    e);
            try {
                Thread.sleep(shardConsumerDispatchPollIntervalMillis);
            } catch (InterruptedException ex) {
                log.info("Worker: sleep interrupted after catching exception ", ex);
            }
        }
        slog.resetInfoLogging();
    }

    private boolean isLeader() {
        return leaderDecider.isLeader(leaseManagementConfig.workerIdentifier());
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
            final MetricsScope metricsScope =
                    MetricsUtil.createMetricsWithOperation(metricsFactory, MULTI_STREAM_TRACKER);

            try {
                final Map<StreamIdentifier, StreamConfig> newStreamConfigMap = streamTracker.streamConfigList().stream()
                        .collect(Collectors.toMap(StreamConfig::streamIdentifier, Function.identity()));
                // This is done to ensure that we clean up the stale streams lingering in the lease table.
                // Only sync from lease table again if the currentStreamConfigMap and newStreamConfigMap contain
                // different set of streams and Leader has not synced the leases yet
                // or this is the first app bootstrap.
                if ((!leaderSynced.get() && !newStreamConfigMap.keySet().equals(currentStreamConfigMap.keySet()))
                        || !leasesSyncedOnAppInit) {
                    log.info("Syncing leases for leader to catch up");
                    final List<MultiStreamLease> leaseTableLeases = fetchMultiStreamLeases();
                    syncStreamsFromLeaseTableOnAppInit(leaseTableLeases);
                    final Set<StreamIdentifier> streamsFromLeaseTable = leaseTableLeases.stream()
                            .map(lease -> StreamIdentifier.multiStreamInstance(lease.streamIdentifier()))
                            .collect(Collectors.toSet());
                    // Remove stream from currentStreamConfigMap if this stream in not in the lease table and
                    // newStreamConfigMap.
                    // This means that the leases have already been deleted by the last leader.
                    currentStreamConfigMap.keySet().stream()
                            .filter(streamIdentifier -> !newStreamConfigMap.containsKey(streamIdentifier)
                                    && !streamsFromLeaseTable.contains(streamIdentifier))
                            .forEach(stream -> {
                                log.info(
                                        "Removing stream {} from currentStreamConfigMap due to not being active",
                                        stream);
                                currentStreamConfigMap.remove(stream);
                                staleStreamDeletionMap.remove(stream);
                                streamsSynced.add(stream);
                            });
                }
                leasesSyncedOnAppInit = true;

                // For new streams discovered, do a shard sync and update the currentStreamConfigMap
                for (StreamIdentifier streamIdentifier : newStreamConfigMap.keySet()) {
                    if (!currentStreamConfigMap.containsKey(streamIdentifier)) {
                        final StreamConfig streamConfig = newStreamConfigMap.get(streamIdentifier);
                        log.info("Found new stream to process: {}. Syncing shards of that stream.", streamConfig);
                        ShardSyncTaskManager shardSyncTaskManager = createOrGetShardSyncTaskManager(streamConfig);
                        shardSyncTaskManager.submitShardSyncTask();
                        currentStreamConfigMap.put(streamIdentifier, streamConfig);
                        streamsSynced.add(streamIdentifier);
                    } else {
                        log.debug("{} is already being processed - skipping shard sync.", streamIdentifier);
                    }
                }

                final Consumer<StreamIdentifier> enqueueStreamLeaseDeletionOperation = streamIdentifier -> {
                    if (!newStreamConfigMap.containsKey(streamIdentifier)) {
                        staleStreamDeletionMap.putIfAbsent(streamIdentifier, Instant.now());
                    }
                };
                if (formerStreamsLeasesDeletionStrategy.leaseDeletionType()
                        == FORMER_STREAMS_AUTO_DETECTION_DEFERRED_DELETION) {
                    // Now, we are identifying the stale/old streams and enqueuing it for deferred deletion.
                    // It is assumed that all the workers will always have the latest and consistent snapshot of streams
                    // from the multiStreamTracker.
                    //
                    // The following streams transition state among two workers are NOT considered safe, where Worker 2,
                    // on
                    // initialization learn about D from lease table and delete the leases for D, as it is not available
                    // in its latest MultiStreamTracker.
                    // Worker 1 : A,B,C -> A,B,C,D (latest)
                    // Worker 2 : BOOTS_UP -> A,B,C (stale)
                    //
                    // The following streams transition state among two workers are NOT considered safe, where Worker 2
                    // might
                    // end up deleting the leases for A and D and lose progress made so far.
                    // Worker 1 : A,B,C -> A,B,C,D (latest)
                    // Worker 2 : A,B,C -> B,C (stale/partial)
                    //
                    // In order to give workers with stale stream info, sufficient time to learn about the new streams
                    // before attempting to delete it, we will be deferring the leases deletion based on the
                    // defer time period.
                    currentStreamConfigMap.keySet().forEach(enqueueStreamLeaseDeletionOperation);
                } else if (formerStreamsLeasesDeletionStrategy.leaseDeletionType()
                        == StreamsLeasesDeletionType.PROVIDED_STREAMS_DEFERRED_DELETION) {
                    Optional.ofNullable(formerStreamsLeasesDeletionStrategy.streamIdentifiersForLeaseCleanup())
                            .ifPresent(streamIdentifiers ->
                                    streamIdentifiers.forEach(enqueueStreamLeaseDeletionOperation));
                } else {
                    // Remove the old/stale streams identified through the new and existing streams list, without
                    // cleaning up their leases. Disabling deprecated shard sync + lease cleanup through a flag.
                    Iterator<StreamIdentifier> currentSetOfStreamsIter =
                            currentStreamConfigMap.keySet().iterator();
                    while (currentSetOfStreamsIter.hasNext()) {
                        StreamIdentifier streamIdentifier = currentSetOfStreamsIter.next();
                        if (!newStreamConfigMap.containsKey(streamIdentifier)) {
                            if (SHOULD_DO_LEASE_SYNC_FOR_OLD_STREAMS) {
                                log.info(
                                        "Found old/deleted stream : {}. Triggering shard sync. Removing from tracked active streams.",
                                        streamIdentifier);
                                ShardSyncTaskManager shardSyncTaskManager =
                                        createOrGetShardSyncTaskManager(currentStreamConfigMap.get(streamIdentifier));
                                shardSyncTaskManager.submitShardSyncTask();
                            } else {
                                log.info(
                                        "Found old/deleted stream : {}. Removing from tracked active streams, but not cleaning up leases,"
                                                + " as part of this workflow",
                                        streamIdentifier);
                            }
                            currentSetOfStreamsIter.remove();
                            streamsSynced.add(streamIdentifier);
                        }
                    }
                }

                final Duration waitPeriodToDeleteOldStreams =
                        formerStreamsLeasesDeletionStrategy.waitPeriodToDeleteFormerStreams();
                // Now let's scan the streamIdentifiersForLeaseCleanup eligible for deferred deletion and delete them.
                // StreamIdentifiers are eligible for deletion only when the deferment period has elapsed and
                // the streamIdentifiersForLeaseCleanup are not present in the latest snapshot.
                final Map<Boolean, Set<StreamIdentifier>> staleStreamIdDeletionDecisionMap =
                        staleStreamDeletionMap.keySet().stream()
                                .collect(
                                        Collectors.partitioningBy(newStreamConfigMap::containsKey, Collectors.toSet()));
                final Set<StreamIdentifier> staleStreamIdsToBeDeleted =
                        staleStreamIdDeletionDecisionMap.get(false).stream()
                                .filter(streamIdentifier ->
                                        Duration.between(staleStreamDeletionMap.get(streamIdentifier), Instant.now())
                                                        .toMillis()
                                                >= waitPeriodToDeleteOldStreams.toMillis())
                                .collect(Collectors.toSet());
                // These are the streams which are deleted in Kinesis and we encounter resource not found during
                // shardSyncTask. This is applicable in MultiStreamMode only, in case of SingleStreamMode, store will
                // not have any data.
                // Filter streams based on newStreamConfigMap so that we don't override input to KCL in any case.
                final Set<StreamIdentifier> deletedStreamSet =
                        this.deletedStreamListProvider.purgeAllDeletedStream().stream()
                                .filter(streamIdentifier -> !newStreamConfigMap.containsKey(streamIdentifier))
                                .collect(Collectors.toSet());
                if (deletedStreamSet.size() > 0) {
                    log.info("Stale streams to delete: {}", deletedStreamSet);
                    staleStreamIdsToBeDeleted.addAll(deletedStreamSet);
                }
                final Set<StreamIdentifier> deletedStreamsLeases = deleteMultiStreamLeases(staleStreamIdsToBeDeleted);
                streamsSynced.addAll(deletedStreamsLeases);

                // Purge the active streams from stale streams list.
                final Set<StreamIdentifier> staleStreamIdsToBeRevived = staleStreamIdDeletionDecisionMap.get(true);
                removeStreamsFromStaleStreamsList(staleStreamIdsToBeRevived);

                if (!staleStreamDeletionMap.isEmpty()) {
                    log.warn(
                            "Streams enqueued for deletion for lease table cleanup along with their scheduled time for deletion: {} ",
                            staleStreamDeletionMap.entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()
                                            .plus(waitPeriodToDeleteOldStreams))));
                }

                streamSyncWatch.reset().start();

                MetricsUtil.addCount(
                        metricsScope, ACTIVE_STREAMS_COUNT, newStreamConfigMap.size(), MetricsLevel.SUMMARY);
                MetricsUtil.addCount(
                        metricsScope,
                        PENDING_STREAMS_DELETION_COUNT,
                        staleStreamDeletionMap.size(),
                        MetricsLevel.SUMMARY);
                MetricsUtil.addCount(
                        metricsScope, NON_EXISTING_STREAM_DELETE_COUNT, deletedStreamSet.size(), MetricsLevel.SUMMARY);
                MetricsUtil.addCount(
                        metricsScope, DELETED_STREAMS_COUNT, deletedStreamsLeases.size(), MetricsLevel.SUMMARY);
            } finally {
                MetricsUtil.endScope(metricsScope);
            }
        }
        return streamsSynced;
    }

    @VisibleForTesting
    boolean shouldSyncStreamsNow() {
        return isMultiStreamMode && (streamSyncWatch.elapsed(TimeUnit.MILLISECONDS) > NEW_STREAM_CHECK_INTERVAL_MILLIS);
    }

    @VisibleForTesting
    void syncStreamsFromLeaseTableOnAppInit(List<MultiStreamLease> leases) {
        leases.stream()
                .map(lease -> StreamIdentifier.multiStreamInstance(lease.streamIdentifier()))
                .filter(streamIdentifier -> !currentStreamConfigMap.containsKey(streamIdentifier))
                .forEach(streamIdentifier -> {
                    final StreamConfig streamConfig = streamTracker.createStreamConfig(streamIdentifier);
                    currentStreamConfigMap.put(streamIdentifier, streamConfig);
                    log.info("Cached {}", streamConfig);
                });
    }

    private List<MultiStreamLease> fetchMultiStreamLeases()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        return (List<MultiStreamLease>)
                ((List) leaseCoordinator.leaseRefresher().listLeases());
    }

    private void removeStreamsFromStaleStreamsList(Set<StreamIdentifier> streamIdentifiers) {
        for (StreamIdentifier streamIdentifier : streamIdentifiers) {
            staleStreamDeletionMap.remove(streamIdentifier);
        }
    }

    private Set<StreamIdentifier> deleteMultiStreamLeases(Set<StreamIdentifier> streamIdentifiers)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        if (streamIdentifiers.isEmpty()) {
            return Collections.emptySet();
        }
        log.info("Deleting streams: {}", streamIdentifiers);
        final Set<StreamIdentifier> streamsSynced = new HashSet<>();
        final List<MultiStreamLease> leases = fetchMultiStreamLeases();
        final Map<String, List<MultiStreamLease>> streamIdToShardsMap = leases.stream()
                .collect(Collectors.groupingBy(
                        MultiStreamLease::streamIdentifier, Collectors.toCollection(ArrayList::new)));
        for (StreamIdentifier streamIdentifier : streamIdentifiers) {
            log.warn("Found old/deleted stream: {}. Directly deleting leases of this stream.", streamIdentifier);
            // Removing streamIdentifier from this map so PSSM doesn't think there is a hole in the stream while
            // scheduler attempts to delete the stream if the stream is taking longer to delete. If deletion fails
            // it will be retried again since stream will still show up in the staleStreamDeletionMap.
            // It is fine for PSSM to detect holes and it should not do shardsync because it takes few iterations
            // to breach the hole confidence interval threshold.
            currentStreamConfigMap.remove(streamIdentifier);
            // Deleting leases will cause the workers to shutdown the record processors for these shards.
            if (deleteMultiStreamLeases(streamIdToShardsMap.get(streamIdentifier.serialize()))) {
                staleStreamDeletionMap.remove(streamIdentifier);
                streamsSynced.add(streamIdentifier);
            }
        }
        if (!streamsSynced.isEmpty()) {
            // map keys are StreamIdentifiers, which are members of StreamConfig, and therefore redundant
            log.info("Streams retained post-deletion: {}", currentStreamConfigMap.values());
        }
        return streamsSynced;
    }

    private boolean deleteMultiStreamLeases(List<MultiStreamLease> leases) {
        if (leases != null) {
            for (MultiStreamLease lease : leases) {
                try {
                    leaseRefresher.deleteLease(lease);
                } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
                    log.error(
                            "Unable to delete stale stream lease {}. Skipping further deletions for this stream. Will retry later.",
                            lease.leaseKey(),
                            e);
                    return false;
                }
            }
        }
        return true;
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
     * Requests a graceful shutdown of the worker, notifying record processors
     * of the impending shutdown. This gives the record processor a final chance to
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
    public CompletableFuture<Boolean> startGracefulShutdown() {
        synchronized (this) {
            if (gracefulShutdownFuture == null) {
                gracefulShutdownFuture =
                        gracefulShutdownCoordinator.startGracefulShutdown(createGracefulShutdownCallable());
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
                return GracefulShutdownContext.builder()
                        .finalShutdownLatch(finalShutdownLatch)
                        .build();
            }
            CountDownLatch shutdownCompleteLatch = new CountDownLatch(leases.size());
            CountDownLatch notificationCompleteLatch = new CountDownLatch(leases.size());
            for (Lease lease : leases) {
                ShutdownNotification shutdownNotification = new ShardConsumerShutdownNotification(
                        leaseCoordinator, lease, notificationCompleteLatch, shutdownCompleteLatch);
                ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
                ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);
                if (consumer != null && !consumer.isShutdown()) {
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
            return GracefulShutdownContext.builder()
                    .shutdownCompleteLatch(shutdownCompleteLatch)
                    .notificationCompleteLatch(notificationCompleteLatch)
                    .finalShutdownLatch(finalShutdownLatch)
                    .scheduler(this)
                    .build();
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

            migrationStateMachine.shutdown();
            migrationComponentsInitializer.shutdown();
            // Stop lease coordinator, so leases are not renewed or stolen from other workers.
            // Lost leases will force Worker to begin shutdown process for all shard consumers in
            // Worker.run().
            leaseCoordinator.stop();

            // Stop the lease cleanup manager
            leaseCleanupManager.shutdown();

            leaderElectedPeriodicShardSyncManager.stop();
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
        finalShutdownLatch.countDown();
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
                    builder.append(ShardInfo.getLeaseKey(shardInfo));
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
    ShardConsumer createOrGetShardConsumer(
            @NonNull final ShardInfo shardInfo,
            @NonNull final ShardRecordProcessorFactory shardRecordProcessorFactory,
            @NonNull final LeaseCleanupManager leaseCleanupManager) {
        ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);
        // Instantiate a new consumer if we don't have one, or the one we
        // had was from an earlier
        // lease instance (and was shutdown). Don't need to create another
        // one if the shard has been
        // completely processed (shutdown reason terminate).
        if ((consumer == null)
                || (consumer.isShutdown() && consumer.shutdownReason().equals(ShutdownReason.LEASE_LOST))) {
            consumer = buildConsumer(shardInfo, shardRecordProcessorFactory, leaseCleanupManager);
            shardInfoShardConsumerMap.put(shardInfo, consumer);
            slog.infoForce("Created new shardConsumer for : " + shardInfo);
        }
        return consumer;
    }

    private ShardSyncTaskManager createOrGetShardSyncTaskManager(StreamConfig streamConfig) {
        return streamToShardSyncTaskManagerMap.computeIfAbsent(
                streamConfig, s -> shardSyncTaskManagerProvider.apply(s));
    }

    protected ShardConsumer buildConsumer(
            @NonNull final ShardInfo shardInfo,
            @NonNull final ShardRecordProcessorFactory shardRecordProcessorFactory,
            @NonNull final LeaseCleanupManager leaseCleanupManager) {
        ShardRecordProcessorCheckpointer checkpointer =
                coordinatorConfig.coordinatorFactory().createRecordProcessorCheckpointer(shardInfo, checkpoint);
        // The only case where streamName is not available will be when multistreamtracker not set. In this case,
        // get the default stream name for the single stream application.
        final StreamIdentifier streamIdentifier = getStreamIdentifier(shardInfo.streamIdentifierSerOpt());

        // Irrespective of single stream app or multi stream app, streamConfig should always be available.
        // If we have a shardInfo, that is not present in currentStreamConfigMap for whatever reason, then return
        // default stream config
        // to gracefully complete the reading.
        StreamConfig streamConfig = currentStreamConfigMap.get(streamIdentifier);
        if (streamConfig == null) {
            streamConfig = withStreamArn(streamTracker.createStreamConfig(streamIdentifier), getKinesisRegion());
            log.info("Created orphan {}", streamConfig);
        }
        Validate.notNull(streamConfig, "StreamConfig should not be null");
        RecordsPublisher cache =
                retrievalConfig.retrievalFactory().createGetRecordsCache(shardInfo, streamConfig, metricsFactory);
        ShardConsumerArgument argument = new ShardConsumerArgument(
                shardInfo,
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
                hierarchicalShardSyncerProvider.apply(streamConfig),
                metricsFactory,
                leaseCleanupManager,
                schemaRegistryDecoder);
        return new ShardConsumer(
                cache,
                executorService,
                shardInfo,
                lifecycleConfig.logWarningForTaskAfterMillis(),
                argument,
                lifecycleConfig.taskExecutionListener(),
                lifecycleConfig.readTimeoutsToIgnoreBeforeWarning());
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
                    log.debug("Removed consumer for {} as lease has been lost", ShardInfo.getLeaseKey(shard));
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
            ExecutorStateEvent executorStateEvent =
                    diagnosticEventFactory.executorStateEvent(executorService, leaseCoordinator);
            RejectedTaskEvent rejectedTaskEvent = diagnosticEventFactory.rejectedTaskEvent(executorStateEvent, t);
            rejectedTaskEvent.accept(diagnosticEventHandler);
        });
    }

    private void logExecutorState() {
        ExecutorStateEvent executorStateEvent =
                diagnosticEventFactory.executorStateEvent(executorService, leaseCoordinator);
        executorStateEvent.accept(diagnosticEventHandler);
    }

    private StreamIdentifier getStreamIdentifier(Optional<String> streamIdentifierString) {
        final StreamIdentifier streamIdentifier;
        if (streamIdentifierString.isPresent()) {
            streamIdentifier = StreamIdentifier.multiStreamInstance(streamIdentifierString.get());
        } else {
            Validate.isTrue(!isMultiStreamMode, "Should not be in MultiStream Mode");
            streamIdentifier =
                    this.currentStreamConfigMap.values().iterator().next().streamIdentifier();
        }
        Validate.notNull(streamIdentifier, "Stream identifier should not be empty");
        return streamIdentifier;
    }

    private Region getKinesisRegion() {
        return retrievalConfig.kinesisClient().serviceClientConfiguration().region();
    }

    /**
     * Create and return a copy of a {@link StreamConfig} object
     * with {@link StreamIdentifier#streamArnOptional()} populated.
     * Only to be used in multi-stream mode.
     *
     * @param streamConfig The {@link StreamConfig} object to return a copy of.
     * @param kinesisRegion The {@link Region} the stream exists in, to be used for constructing the {@link Arn}.
     * @return A copy of the {@link StreamConfig} with {@link StreamIdentifier#streamArnOptional()} populated.
     */
    private static StreamConfig withStreamArn(
            @NonNull final StreamConfig streamConfig, @NonNull final Region kinesisRegion) {
        Validate.isTrue(
                streamConfig.streamIdentifier().accountIdOptional().isPresent(), "accountId should not be empty");
        Validate.isTrue(
                streamConfig.streamIdentifier().streamCreationEpochOptional().isPresent(),
                "streamCreationEpoch should not be empty");

        log.info(
                "Constructing stream ARN for {} using the Kinesis client's configured region - {}.",
                streamConfig.streamIdentifier(),
                kinesisRegion);

        final StreamIdentifier streamIdentifierWithArn = StreamIdentifier.multiStreamInstance(
                constructStreamArn(
                        kinesisRegion,
                        streamConfig.streamIdentifier().accountIdOptional().get(),
                        streamConfig.streamIdentifier().streamName()),
                streamConfig.streamIdentifier().streamCreationEpochOptional().get());

        return new StreamConfig(
                streamIdentifierWithArn, streamConfig.initialPositionInStreamExtended(), streamConfig.consumerArn());
    }

    @RequiredArgsConstructor
    private class StreamConfigMap extends ConcurrentHashMap<StreamIdentifier, StreamConfig> {
        /**
         * If {@link StreamIdentifier#streamArnOptional()} is present for the provided
         * {@link StreamConfig#streamIdentifier()}, validates that the region in the stream ARN is consistent with the
         * region that the Kinesis client ({@link RetrievalConfig#kinesisClient()}) is configured with.
         * <p>
         * In multi-stream mode, ensures stream ARN is always present by constructing it using the Kinesis client
         * region when {@link StreamIdentifier#streamArnOptional()} is {@link Optional#empty()}.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public StreamConfig put(
                @NonNull final StreamIdentifier streamIdentifier, @NonNull final StreamConfig streamConfig) {
            final Region kinesisRegion = getKinesisRegion();

            return super.put(
                    streamIdentifier,
                    streamConfig
                            .streamIdentifier()
                            .streamArnOptional()
                            .map(streamArn -> {
                                Validate.isTrue(
                                        kinesisRegion
                                                .id()
                                                .equals(streamArn.region().get()),
                                        "The provided streamARN " + streamArn
                                                + " does not match the Kinesis client's configured region - "
                                                + kinesisRegion);
                                return streamConfig;
                            })
                            .orElse(isMultiStreamMode ? withStreamArn(streamConfig, kinesisRegion) : streamConfig));
        }
    }

    /**
     * Logger for suppressing too much INFO logging. To avoid too much logging information Worker will output logging at
     * INFO level for a single pass through the main loop every minute. At DEBUG level it will output all INFO logs on
     * every pass.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class SchedulerLog {

        private final long reportIntervalMillis = TimeUnit.MINUTES.toMillis(1);
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
                if (!log.isDebugEnabled() && !log.isTraceEnabled()) {
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

    /**
     * If WorkerMetricStats list is empty and the disable flag is false, select WorkerMetricStats automatically.
     */
    private void selectWorkerMetricsIfAvailable(
            final WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig) {
        try {
            if (workerUtilizationAwareAssignmentConfig.workerMetricList().isEmpty()
                    && !workerUtilizationAwareAssignmentConfig.disableWorkerMetrics()) {
                workerUtilizationAwareAssignmentConfig.workerMetricList(
                        WorkerMetricsSelector.create().getDefaultWorkerMetrics());
            }
        } catch (final Exception e) {
            log.warn(
                    "Exception encountered during WorkerMetricStats selection. If this is persistent please try setting the "
                            + "WorkerMetricStats explicitly.",
                    e);
        }
    }
}
