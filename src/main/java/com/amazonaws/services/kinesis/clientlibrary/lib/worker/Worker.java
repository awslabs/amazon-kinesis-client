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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.GenericLeaseSelector;
import com.amazonaws.services.kinesis.leases.impl.LeaseCoordinator;
import com.amazonaws.services.kinesis.leases.impl.LeaseRenewer;
import com.amazonaws.services.kinesis.leases.impl.LeaseTaker;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseRenewer;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseTaker;
import com.amazonaws.services.kinesis.leases.interfaces.LeaseSelector;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Worker is the high level class that Kinesis applications use to start processing data. It initializes and oversees
 * different components (e.g. syncing shard and lease information, tracking shard assignments, and processing data from
 * the shards).
 */
public class Worker implements Runnable {

    private static final Log LOG = LogFactory.getLog(Worker.class);

    // Default configs for periodic shard sync
    private static final int SHARD_SYNC_SLEEP_FOR_PERIODIC_SHARD_SYNC = 0;
    private static final int MAX_INITIALIZATION_ATTEMPTS = 20;
    private static final int PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT = 1; //Default for KCL.
    static final long LEASE_TABLE_CHECK_FREQUENCY_MILLIS = 3 * 1000L;
    static final long MIN_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS = 1 * 1000L;
    static final long MAX_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS = 30 * 1000L;

    private static final WorkerStateChangeListener DEFAULT_WORKER_STATE_CHANGE_LISTENER = new NoOpWorkerStateChangeListener();
    private static final LeaseCleanupValidator DEFAULT_LEASE_CLEANUP_VALIDATOR = new KinesisLeaseCleanupValidator();
    private static final LeaseSelector<KinesisClientLease> DEFAULT_LEASE_SELECTOR = new GenericLeaseSelector<KinesisClientLease>();

    private WorkerLog wlog = new WorkerLog();

    private final String applicationName;
    private final IRecordProcessorFactory recordProcessorFactory;
    private final KinesisClientLibConfiguration config;
    private final StreamConfig streamConfig;
    private final InitialPositionInStreamExtended initialPosition;
    private final ICheckpoint checkpointTracker;
    private final long idleTimeInMilliseconds;
    // Backoff time when polling to check if application has finished processing
    // parent shards
    private final long parentShardPollIntervalMillis;
    private final ExecutorService executorService;
    private final IMetricsFactory metricsFactory;
    // Backoff time when running tasks if they encounter exceptions
    private final long taskBackoffTimeMillis;
    private final long failoverTimeMillis;

    private final Optional<Integer> retryGetRecordsInSeconds;
    private final Optional<Integer> maxGetRecordsThreadPool;

    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private final ShardSyncTaskManager shardSyncTaskManager;

    private final ShardPrioritization shardPrioritization;

    private volatile boolean shutdown;
    private volatile long shutdownStartTimeMillis;
    private volatile boolean shutdownComplete = false;
    private final ShardSyncer shardSyncer;

    // Holds consumers for shards the worker is currently tracking. Key is shard
    // info, value is ShardConsumer.
    private ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap = new ConcurrentHashMap<ShardInfo, ShardConsumer>();
    private final boolean cleanupLeasesUponShardCompletion;

    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist;

    /**
     * Used to ensure that only one requestedShutdown is in progress at a time.
     */
    private Future<Boolean> gracefulShutdownFuture;
    @VisibleForTesting
    protected boolean gracefuleShutdownStarted = false;
    @VisibleForTesting
    protected GracefulShutdownCoordinator gracefulShutdownCoordinator = new GracefulShutdownCoordinator();

    private WorkerStateChangeListener workerStateChangeListener;

    // Periodic Shard Sync related fields
    private LeaderDecider leaderDecider;
    private ShardSyncStrategy shardSyncStrategy;
    private PeriodicShardSyncManager leaderElectedPeriodicShardSyncManager;

    /**
     * Constructor.
     *
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config) {
        this(recordProcessorFactory, config, getExecutorService());
    }

    /**
     * Constructor.
     *
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param execService
     *            ExecutorService to use for processing records (support for multi-threaded consumption)
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, ExecutorService execService) {
        this(recordProcessorFactory, config,
                new AmazonKinesisClient(config.getKinesisCredentialsProvider(), config.getKinesisClientConfiguration()),
                new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(),
                        config.getDynamoDBClientConfiguration()),
                new AmazonCloudWatchClient(config.getCloudWatchCredentialsProvider(),
                        config.getCloudWatchClientConfiguration()),
                execService);
    }

    /**
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param metricsFactory
     *            Metrics factory used to emit metrics
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, IMetricsFactory metricsFactory) {
        this(recordProcessorFactory, config, metricsFactory, getExecutorService());
    }

    /**
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param metricsFactory
     *            Metrics factory used to emit metrics
     * @param execService
     *            ExecutorService to use for processing records (support for multi-threaded consumption)
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, IMetricsFactory metricsFactory, ExecutorService execService) {
        this(recordProcessorFactory, config,
                new AmazonKinesisClient(config.getKinesisCredentialsProvider(), config.getKinesisClientConfiguration()),
                new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(),
                        config.getDynamoDBClientConfiguration()),
                metricsFactory, execService);
    }

    /**
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param kinesisClient
     *            Kinesis Client used for fetching data
     * @param dynamoDBClient
     *            DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient
     *            CloudWatch Client for publishing metrics
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, AmazonKinesis kinesisClient, AmazonDynamoDB dynamoDBClient,
            AmazonCloudWatch cloudWatchClient) {
        this(recordProcessorFactory, config, kinesisClient, dynamoDBClient, cloudWatchClient, getExecutorService());
    }

    /**
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param kinesisClient
     *            Kinesis Client used for fetching data
     * @param dynamoDBClient
     *            DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient
     *            CloudWatch Client for publishing metrics
     * @param execService
     *            ExecutorService to use for processing records (support for multi-threaded consumption)
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, AmazonKinesis kinesisClient, AmazonDynamoDB dynamoDBClient,
            AmazonCloudWatch cloudWatchClient, ExecutorService execService) {
        this(recordProcessorFactory, config, kinesisClient, dynamoDBClient, getMetricsFactory(cloudWatchClient, config),
                execService);
    }

    // Backwards compatible constructors
    /**
     * This constructor is for binary compatibility with code compiled against version of the KCL that only have
     * constructors taking "Client" objects.
     *
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param kinesisClient
     *            Kinesis Client used for fetching data
     * @param dynamoDBClient
     *            DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient
     *            CloudWatch Client for publishing metrics
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient, AmazonCloudWatchClient cloudWatchClient) {
        this(recordProcessorFactory, config, (AmazonKinesis) kinesisClient, (AmazonDynamoDB) dynamoDBClient,
                (AmazonCloudWatch) cloudWatchClient);
    }

    /**
     * This constructor is for binary compatibility with code compiled against version of the KCL that only have
     * constructors taking "Client" objects.
     *
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param kinesisClient
     *            Kinesis Client used for fetching data
     * @param dynamoDBClient
     *            DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient
     *            CloudWatch Client for publishing metrics
     * @param execService
     *            ExecutorService to use for processing records (support for multi-threaded consumption)
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient, AmazonCloudWatchClient cloudWatchClient, ExecutorService execService) {
        this(recordProcessorFactory, config, (AmazonKinesis) kinesisClient, (AmazonDynamoDB) dynamoDBClient,
                (AmazonCloudWatch) cloudWatchClient, execService);
    }

    /**
     * This constructor is for binary compatibility with code compiled against version of the KCL that only have
     * constructors taking "Client" objects.
     *
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param kinesisClient
     *            Kinesis Client used for fetching data
     * @param dynamoDBClient
     *            DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory
     *            Metrics factory used to emit metrics
     * @param execService
     *            ExecutorService to use for processing records (support for multi-threaded consumption)
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient, IMetricsFactory metricsFactory, ExecutorService execService) {
        this(recordProcessorFactory, config, (AmazonKinesis) kinesisClient, (AmazonDynamoDB) dynamoDBClient,
                metricsFactory, execService);
    }

    /**
     * @deprecated The access to this constructor will be changed in a future release. The recommended way to create
     * a Worker is to use {@link Builder}
     *
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Client Library configuration
     * @param kinesisClient
     *            Kinesis Client used for fetching data
     * @param dynamoDBClient
     *            DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory
     *            Metrics factory used to emit metrics
     * @param execService
     *            ExecutorService to use for processing records (support for multi-threaded consumption)
     */
    @Deprecated
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config, AmazonKinesis kinesisClient, AmazonDynamoDB dynamoDBClient,
            IMetricsFactory metricsFactory, ExecutorService execService) {
        this(config.getApplicationName(), new V1ToV2RecordProcessorFactoryAdapter(recordProcessorFactory),
                config, getStreamConfig(config, kinesisClient),
                config.getInitialPositionInStreamExtended(), config.getParentShardPollIntervalMillis(),
                config.getShardSyncIntervalMillis(), config.shouldCleanupLeasesUponShardCompletion(), null,
                getLeaseCoordinator(config, dynamoDBClient, metricsFactory)
                        .withInitialLeaseTableReadCapacity(config.getInitialLeaseTableReadCapacity())
                        .withInitialLeaseTableWriteCapacity(config.getInitialLeaseTableWriteCapacity()),
                execService,
                metricsFactory,
                config.getTaskBackoffTimeMillis(),
                config.getFailoverTimeMillis(),
                config.getSkipShardSyncAtWorkerInitializationIfLeasesExist(),
                config.getShardPrioritizationStrategy(),
                config.getRetryGetRecordsInSeconds(),
                config.getMaxGetRecordsThreadPool(),
                DEFAULT_WORKER_STATE_CHANGE_LISTENER, DEFAULT_LEASE_CLEANUP_VALIDATOR, null, null);

        // If a region name was explicitly specified, use it as the region for Amazon Kinesis and Amazon DynamoDB.
        if (config.getRegionName() != null) {
            setField(kinesisClient, "region", kinesisClient::setRegion, RegionUtils.getRegion(config.getRegionName()));
            setField(dynamoDBClient, "region", dynamoDBClient::setRegion, RegionUtils.getRegion(config.getRegionName()));
        }
        // If a dynamoDB endpoint was explicitly specified, use it to set the DynamoDB endpoint.
        if (config.getDynamoDBEndpoint() != null) {
            setField(dynamoDBClient, "endpoint", dynamoDBClient::setEndpoint, config.getDynamoDBEndpoint());
        }
        // If a kinesis endpoint was explicitly specified, use it to set the region of kinesis.
        if (config.getKinesisEndpoint() != null) {
            setField(kinesisClient, "endpoint", kinesisClient::setEndpoint, config.getKinesisEndpoint());
        }
    }

    /**
     * @param applicationName
     *            Name of the Kinesis application
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @paran config
     *            Kinesis Library configuration
     * @param streamConfig
     *            Stream configuration
     * @param initialPositionInStream
     *            One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. The KinesisClientLibrary will start fetching data from
     *            this location in the stream when an application starts up for the first time and there are no
     *            checkpoints. If there are checkpoints, we start from the checkpoint position.
     * @param parentShardPollIntervalMillis
     *            Wait for this long between polls to check if parent shards are done
     * @param shardSyncIdleTimeMillis
     *            Time between tasks to sync leases and Kinesis shards
     * @param cleanupLeasesUponShardCompletion
     *            Clean up shards we've finished processing (don't wait till they expire in Kinesis)
     * @param checkpoint
     *            Used to get/set checkpoints
     * @param leaseCoordinator
     *            Lease coordinator (coordinates currently owned leases)
     * @param execService
     *            ExecutorService to use for processing records (support for multi-threaded consumption)
     * @param metricsFactory
     *            Metrics factory used to emit metrics
     * @param taskBackoffTimeMillis
     *            Backoff period when tasks encounter an exception
     * @param shardPrioritization
     *            Provides prioritization logic to decide which available shards process first
     */
    // NOTE: This has package level access solely for testing
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    Worker(String applicationName, IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
            StreamConfig streamConfig, InitialPositionInStreamExtended initialPositionInStream, long parentShardPollIntervalMillis,
            long shardSyncIdleTimeMillis, boolean cleanupLeasesUponShardCompletion, ICheckpoint checkpoint,
            KinesisClientLibLeaseCoordinator leaseCoordinator, ExecutorService execService,
            IMetricsFactory metricsFactory, long taskBackoffTimeMillis, long failoverTimeMillis,
            boolean skipShardSyncAtWorkerInitializationIfLeasesExist, ShardPrioritization shardPrioritization) {
        this(applicationName, recordProcessorFactory, config, streamConfig, initialPositionInStream, parentShardPollIntervalMillis,
                shardSyncIdleTimeMillis, cleanupLeasesUponShardCompletion, checkpoint, leaseCoordinator, execService,
                metricsFactory, taskBackoffTimeMillis, failoverTimeMillis, skipShardSyncAtWorkerInitializationIfLeasesExist,
                shardPrioritization, Optional.empty(), Optional.empty(), DEFAULT_WORKER_STATE_CHANGE_LISTENER,
                DEFAULT_LEASE_CLEANUP_VALIDATOR, null, null);
    }

    /**
     * @param applicationName
     *            Name of the Kinesis application
     * @param recordProcessorFactory
     *            Used to get record processor instances for processing data from shards
     * @param config
     *            Kinesis Library Configuration
     * @param streamConfig
     *            Stream configuration
     * @param initialPositionInStream
     *            One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. The KinesisClientLibrary will start fetching data from
     *            this location in the stream when an application starts up for the first time and there are no
     *            checkpoints. If there are checkpoints, we start from the checkpoint position.
     * @param parentShardPollIntervalMillis
     *            Wait for this long between polls to check if parent shards are done
     * @param shardSyncIdleTimeMillis
     *            Time between tasks to sync leases and Kinesis shards
     * @param cleanupLeasesUponShardCompletion
     *            Clean up shards we've finished processing (don't wait till they expire in Kinesis)
     * @param checkpoint
     *            Used to get/set checkpoints
     * @param leaseCoordinator
     *            Lease coordinator (coordinates currently owned leases)
     * @param execService
     *            ExecutorService to use for processing records (support for multi-threaded consumption)
     * @param metricsFactory
     *            Metrics factory used to emit metrics
     * @param taskBackoffTimeMillis
     *            Backoff period when tasks encounter an exception
     * @param shardPrioritization
     *            Provides prioritization logic to decide which available shards process first
     * @param retryGetRecordsInSeconds
     *            Time in seconds to wait before the worker retries to get a record.
     * @param maxGetRecordsThreadPool
     *            Max number of threads in the getRecords thread pool.
     * @param leaseCleanupValidator
     *            leaseCleanupValidator instance used to validate leases
     * @param leaderDecider
     *            leaderDecider instance used elect shard sync leaders
     * @param periodicShardSyncManager
     *            manages periodic shard sync tasks
     */
    // NOTE: This has package level access solely for testing
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    Worker(String applicationName, IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config, StreamConfig streamConfig,
            InitialPositionInStreamExtended initialPositionInStream, long parentShardPollIntervalMillis,
            long shardSyncIdleTimeMillis, boolean cleanupLeasesUponShardCompletion, ICheckpoint checkpoint,
            KinesisClientLibLeaseCoordinator leaseCoordinator, ExecutorService execService,
            IMetricsFactory metricsFactory, long taskBackoffTimeMillis, long failoverTimeMillis,
            boolean skipShardSyncAtWorkerInitializationIfLeasesExist, ShardPrioritization shardPrioritization,
            Optional<Integer> retryGetRecordsInSeconds, Optional<Integer> maxGetRecordsThreadPool, WorkerStateChangeListener workerStateChangeListener,
            LeaseCleanupValidator leaseCleanupValidator, LeaderDecider leaderDecider, PeriodicShardSyncManager periodicShardSyncManager) {
        this(applicationName, recordProcessorFactory, config, streamConfig, initialPositionInStream,
                parentShardPollIntervalMillis, shardSyncIdleTimeMillis, cleanupLeasesUponShardCompletion, checkpoint,
                leaseCoordinator, execService, metricsFactory, taskBackoffTimeMillis, failoverTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist, shardPrioritization, retryGetRecordsInSeconds,
                maxGetRecordsThreadPool, workerStateChangeListener, new KinesisShardSyncer(leaseCleanupValidator),
                leaderDecider, periodicShardSyncManager);
    }

    Worker(String applicationName, IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
            StreamConfig streamConfig, InitialPositionInStreamExtended initialPositionInStream,
            long parentShardPollIntervalMillis, long shardSyncIdleTimeMillis, boolean cleanupLeasesUponShardCompletion,
            ICheckpoint checkpoint, KinesisClientLibLeaseCoordinator leaseCoordinator, ExecutorService execService,
            IMetricsFactory metricsFactory, long taskBackoffTimeMillis, long failoverTimeMillis,
            boolean skipShardSyncAtWorkerInitializationIfLeasesExist, ShardPrioritization shardPrioritization,
            Optional<Integer> retryGetRecordsInSeconds, Optional<Integer> maxGetRecordsThreadPool,
            WorkerStateChangeListener workerStateChangeListener, ShardSyncer shardSyncer, LeaderDecider leaderDecider,
            PeriodicShardSyncManager periodicShardSyncManager) {
        this.applicationName = applicationName;
        this.recordProcessorFactory = recordProcessorFactory;
        this.config = config;
        this.streamConfig = streamConfig;
        this.initialPosition = initialPositionInStream;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.checkpointTracker = checkpoint != null ? checkpoint : leaseCoordinator;
        this.idleTimeInMilliseconds = streamConfig.getIdleTimeInMilliseconds();
        this.executorService = execService;
        this.leaseCoordinator = leaseCoordinator;
        this.metricsFactory = metricsFactory;
        this.shardSyncer = shardSyncer;
        this.shardSyncTaskManager = new ShardSyncTaskManager(streamConfig.getStreamProxy(), leaseCoordinator.getLeaseManager(),
                initialPositionInStream, cleanupLeasesUponShardCompletion, config.shouldIgnoreUnexpectedChildShards(),
                shardSyncIdleTimeMillis, metricsFactory, executorService, shardSyncer);
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
        this.failoverTimeMillis = failoverTimeMillis;
        this.skipShardSyncAtWorkerInitializationIfLeasesExist = skipShardSyncAtWorkerInitializationIfLeasesExist;
        this.shardPrioritization = shardPrioritization;
        this.retryGetRecordsInSeconds = retryGetRecordsInSeconds;
        this.maxGetRecordsThreadPool = maxGetRecordsThreadPool;
        this.workerStateChangeListener = workerStateChangeListener;
        workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.CREATED);
        createShardSyncStrategy(config.getShardSyncStrategyType(), leaderDecider, periodicShardSyncManager);
    }

    /**
     * Create shard sync strategy and corresponding {@link LeaderDecider} based on provided configs. PERIODIC
     * {@link ShardSyncStrategyType} honors custom leaderDeciders for leader election strategy. All other
     * {@link ShardSyncStrategyType}s permit only a default single-leader strategy.
     */
    private void createShardSyncStrategy(ShardSyncStrategyType strategyType,
                                         LeaderDecider leaderDecider,
                                         PeriodicShardSyncManager periodicShardSyncManager) {
        switch (strategyType) {
            case PERIODIC:
                this.leaderDecider = getOrCreateLeaderDecider(leaderDecider);
                this.leaderElectedPeriodicShardSyncManager =
                        getOrCreatePeriodicShardSyncManager(periodicShardSyncManager);
                this.shardSyncStrategy = createPeriodicShardSyncStrategy();
                break;
            case SHARD_END:
            default:
                if (leaderDecider != null) {
                    LOG.warn("LeaderDecider cannot be customized with non-PERIODIC shard sync strategy type. Using " +
                             "default LeaderDecider.");
                }
                this.leaderDecider = getOrCreateLeaderDecider(null);
                this.leaderElectedPeriodicShardSyncManager =
                        getOrCreatePeriodicShardSyncManager(periodicShardSyncManager);
                this.shardSyncStrategy = createShardEndShardSyncStrategy();
        }

        LOG.info("Shard sync strategy determined as " + shardSyncStrategy.getStrategyType().toString());
    }

    private static KinesisClientLibLeaseCoordinator getLeaseCoordinator(KinesisClientLibConfiguration config,
            AmazonDynamoDB dynamoDBClient, IMetricsFactory metricsFactory) {
        return new KinesisClientLibLeaseCoordinator(
                new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode()), DEFAULT_LEASE_SELECTOR,
                config.getWorkerIdentifier(), config.getFailoverTimeMillis(), config.getEpsilonMillis(),
                config.getMaxLeasesForWorker(), config.getMaxLeasesToStealAtOneTime(),
                config.getMaxLeaseRenewalThreads(), metricsFactory);
    }

    private static StreamConfig getStreamConfig(KinesisClientLibConfiguration config, AmazonKinesis kinesisClient) {
        return new StreamConfig(new KinesisProxy(config, kinesisClient), config.getMaxRecords(),
                config.getIdleTimeBetweenReadsInMillis(), config.shouldCallProcessRecordsEvenForEmptyRecordList(),
                config.shouldValidateSequenceNumberBeforeCheckpointing(), config.getInitialPositionInStreamExtended());
    }

    /**
     * @return the applicationName
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * @return the leaseCoordinator
     */
    KinesisClientLibLeaseCoordinator getLeaseCoordinator(){
        return leaseCoordinator;
    }

    /**
     * @return the leaderDecider
     */
    LeaderDecider getLeaderDecider() {
        return leaderDecider;
    }

    /**
     * @return the leaderElectedPeriodicShardSyncManager
     */
    PeriodicShardSyncManager getPeriodicShardSyncManager() {
        return leaderElectedPeriodicShardSyncManager;
    }

    /**
     * Start consuming data from the stream, and pass it to the application record processors.
     */
    public void run() {
        if (shutdown) {
            return;
        }

        try {
            initialize();
            LOG.info("Initialization complete. Starting worker loop.");
        } catch (RuntimeException e1) {
            LOG.error("Unable to initialize after " + MAX_INITIALIZATION_ATTEMPTS + " attempts. Shutting down.", e1);
            shutdown();
        }

        while (!shouldShutdown()) {
            runProcessLoop();
        }

        finalShutdown();
        LOG.info("Worker loop is complete. Exiting from worker.");
    }

    @VisibleForTesting
    void runProcessLoop() {
        try {
            boolean foundCompletedShard = false;
            Set<ShardInfo> assignedShards = new HashSet<>();
            for (ShardInfo shardInfo : getShardInfoForAssignments()) {
                ShardConsumer shardConsumer = createOrGetShardConsumer(shardInfo, recordProcessorFactory);
                if (shardConsumer.isShutdown() && shardConsumer.getShutdownReason().equals(ShutdownReason.TERMINATE)) {
                    foundCompletedShard = true;
                } else {
                    shardConsumer.consumeShard();
                }
                assignedShards.add(shardInfo);
            }

            if (foundCompletedShard) {
                shardSyncStrategy.onFoundCompletedShard();
            }

            // clean up shard consumers for unassigned shards
            cleanupShardConsumers(assignedShards);

            wlog.info("Sleeping ...");
            Thread.sleep(idleTimeInMilliseconds);
        } catch (Exception e) {
            LOG.error(String.format("Worker.run caught exception, sleeping for %s milli seconds!",
                    String.valueOf(idleTimeInMilliseconds)), e);
            try {
                Thread.sleep(idleTimeInMilliseconds);
            } catch (InterruptedException ex) {
                LOG.info("Worker: sleep interrupted after catching exception ", ex);
            }
        }
        wlog.resetInfoLogging();
    }

    private void initialize() {
        workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.INITIALIZING);
        boolean isDone = false;
        Exception lastException = null;

        for (int i = 0; (!isDone) && (i < MAX_INITIALIZATION_ATTEMPTS); i++) {
            try {
                LOG.info("Initialization attempt " + (i + 1));
                LOG.info("Initializing LeaseCoordinator");
                leaseCoordinator.initialize();

                // Perform initial lease sync if configs allow it, with jitter.
                if (shouldInitiateLeaseSync()) {
                    LOG.info(config.getWorkerIdentifier() + " worker is beginning initial lease sync.");
                    TaskResult result = leaderElectedPeriodicShardSyncManager.syncShardsOnce();
                    if (result.getException() != null) {
                        throw result.getException();
                    }
                }

                // If we reach this point, then we either skipped the lease sync or did not have any exception for the
                // shard sync in the previous attempt.
                if (!leaseCoordinator.isRunning()) {
                    LOG.info("Starting LeaseCoordinator");
                    leaseCoordinator.start();
                } else {
                    LOG.info("LeaseCoordinator is already running. No need to start it.");
                }

                // All shard sync strategies' initialization handlers should begin a periodic shard sync. For
                // PeriodicShardSync strategy, this is the main shard sync loop. For ShardEndShardSync and other
                // shard sync strategies, this serves as an auditor background process.
                shardSyncStrategy.onWorkerInitialization();
                isDone = true;

            } catch (LeasingException e) {
                LOG.error("Caught exception when initializing LeaseCoordinator", e);
                lastException = e;
            } catch (Exception e) {
                lastException = e;
            }

            try {
                Thread.sleep(parentShardPollIntervalMillis);
            } catch (InterruptedException e) {
                LOG.debug("Sleep interrupted while initializing worker.");
            }
        }

        if (!isDone) {
            leaderElectedPeriodicShardSyncManager.stop();
            throw new RuntimeException(lastException);

        }
        workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.STARTED);
    }

    @VisibleForTesting
    boolean shouldInitiateLeaseSync() throws InterruptedException, DependencyException, InvalidStateException,
            ProvisionedThroughputException {

        final ILeaseManager leaseManager = leaseCoordinator.getLeaseManager();
        if (skipShardSyncAtWorkerInitializationIfLeasesExist && !leaseManager.isLeaseTableEmpty()) {
            LOG.info("Skipping shard sync because getSkipShardSyncAtWorkerInitializationIfLeasesExist config is set " +
                    "to TRUE and lease table is not empty.");
            return false;
        }

        final long waitTime = ThreadLocalRandom.current().nextLong(MIN_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS,
                MAX_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS);
        final long waitUntil = System.currentTimeMillis() + waitTime;

        boolean shouldInitiateLeaseSync = true;
        while (System.currentTimeMillis() < waitUntil && (shouldInitiateLeaseSync = leaseManager.isLeaseTableEmpty())) {
            // Check every 3 seconds if lease table is still empty, to minimize contention between all workers
            // bootstrapping from empty lease table at the same time.
            LOG.info("Lease table is still empty. Checking again in " + LEASE_TABLE_CHECK_FREQUENCY_MILLIS + " ms.");
            Thread.sleep(LEASE_TABLE_CHECK_FREQUENCY_MILLIS);
        }

        return shouldInitiateLeaseSync;
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
     * Starts the requestedShutdown process, and returns a future that can be used to track the process.
     *
     * This is deprecated in favor of {@link #startGracefulShutdown()}, which returns a more complete future, and
     * indicates the process behavior
     *
     * @return a future that will be set once shutdown is completed.
     */
    @Deprecated
    public Future<Void> requestShutdown() {

        Future<Boolean> requestedShutdownFuture = startGracefulShutdown();

        return new Future<Void>() {

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return requestedShutdownFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return requestedShutdownFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return requestedShutdownFuture.isDone();
            }

            @Override
            public Void get() throws InterruptedException, ExecutionException {
                requestedShutdownFuture.get();
                return null;
            }

            @Override
            public Void get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                requestedShutdownFuture.get(timeout, unit);
                return null;
            }
        };
    }

    /**
     * Requests a graceful shutdown of the worker, notifying record processors, that implement
     * {@link IShutdownNotificationAware}, of the impending shutdown. This gives the record processor a final chance to
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
     * with {@link ShutdownReason#ZOMBIE}</li>
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
        if (isShutdownComplete()) {
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

            Collection<KinesisClientLease> leases = leaseCoordinator.getAssignments();
            if (leases == null || leases.isEmpty()) {
                //
                // If there are no leases notification is already completed, but we still need to shutdown the worker.
                //
                this.shutdown();
                return GracefulShutdownContext.SHUTDOWN_ALREADY_COMPLETED;
            }
            CountDownLatch shutdownCompleteLatch = new CountDownLatch(leases.size());
            CountDownLatch notificationCompleteLatch = new CountDownLatch(leases.size());
            for (KinesisClientLease lease : leases) {
                ShutdownNotification shutdownNotification = new ShardConsumerShutdownNotification(leaseCoordinator,
                        lease, notificationCompleteLatch, shutdownCompleteLatch);
                ShardInfo shardInfo = KinesisClientLibLeaseCoordinator.convertLeaseToAssignment(lease);
                ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);

                if (consumer == null || ConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE.equals(consumer.getCurrentState())) {
                    //
                    // CASE1: There is a race condition between retrieving the current assignments, and creating the
                    // notification. If the a lease is lost in between these two points, we explicitly decrement the
                    // notification latches to clear the shutdown.
                    //
                    // CASE2: The shard consumer is in SHUTDOWN_COMPLETE state and will not decrement the latches by itself.
                    //
                    notificationCompleteLatch.countDown();
                    shutdownCompleteLatch.countDown();
                } else {
                    consumer.notifyShutdownRequested(shutdownNotification);
                }
            }
            return new GracefulShutdownContext(shutdownCompleteLatch, notificationCompleteLatch, this);
        };
    }

    boolean isShutdownComplete() {
        return shutdownComplete;
    }

    ConcurrentMap<ShardInfo, ShardConsumer> getShardInfoShardConsumerMap() {
        return shardInfoShardConsumerMap;
    }

    WorkerStateChangeListener getWorkerStateChangeListener() {
        return workerStateChangeListener;
    }

    /**
     * Signals worker to shutdown. Worker will try initiating shutdown of all record processors. Note that if executor
     * services were passed to the worker by the user, worker will not attempt to shutdown those resources.
     *
     * <h2>Shutdown Process</h2> When called this will start shutdown of the record processor, and eventually shutdown
     * the worker itself.
     * <ol>
     * <li>Call to start shutdown invoked</li>
     * <li>Lease coordinator told to onWorkerShutDown taking leases, and to drop existing leases.</li>
     * <li>Worker discovers record processors that no longer have leases.</li>
     * <li>Worker triggers shutdown with state {@link ShutdownReason#ZOMBIE}.</li>
     * <li>Once all record processors are shutdown, worker terminates owned resources.</li>
     * <li>Shutdown complete.</li>
     * </ol>
     */
    public void shutdown() {
        if (shutdown) {
            LOG.warn("Shutdown requested a second time.");
            return;
        }
        LOG.info("Worker shutdown requested.");

        // Set shutdown flag, so Worker.run can start shutdown process.
        shutdown = true;
        shutdownStartTimeMillis = System.currentTimeMillis();

        // Stop lease coordinator, so leases are not renewed or stolen from other workers.
        // Lost leases will force Worker to begin shutdown process for all shard consumers in
        // Worker.run().
        leaseCoordinator.stop();
        // Stop the periodicShardSyncManager for the worker
        if (shardSyncStrategy != null) {
            shardSyncStrategy.onWorkerShutDown();
        }
        workerStateChangeListener.onWorkerStateChange(WorkerStateChangeListener.WorkerState.SHUT_DOWN);
    }

    /**
     * Perform final shutdown related tasks for the worker including shutting down worker owned executor services,
     * threads, etc.
     */
    private void finalShutdown() {
        LOG.info("Starting worker's final shutdown.");

        if (executorService instanceof WorkerThreadPoolExecutor) {
            // This should interrupt all active record processor tasks.
            executorService.shutdownNow();
        }
        if (metricsFactory instanceof WorkerCWMetricsFactory) {
            ((CWMetricsFactory) metricsFactory).shutdown();
        }
        shutdownComplete = true;
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
            LOG.error("Worker executor service has been shutdown, so record processors cannot be shutdown.");
            return true;
        }
        if (shutdown) {
            if (shardInfoShardConsumerMap.isEmpty()) {
                LOG.info("All record processors have been shutdown successfully.");
                return true;
            }
            if ((System.currentTimeMillis() - shutdownStartTimeMillis) >= failoverTimeMillis) {
                LOG.info("Lease failover time is reached, so forcing shutdown.");
                return true;
            }
        }
        return false;
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
    ShardConsumer createOrGetShardConsumer(ShardInfo shardInfo, IRecordProcessorFactory processorFactory) {
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

    protected ShardConsumer buildConsumer(ShardInfo shardInfo, IRecordProcessorFactory processorFactory) {
        IRecordProcessor recordProcessor = processorFactory.createProcessor();

        return new ShardConsumer(shardInfo,
                streamConfig,
                checkpointTracker,
                recordProcessor,
                leaseCoordinator,
                parentShardPollIntervalMillis,
                cleanupLeasesUponShardCompletion,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                retryGetRecordsInSeconds,
                maxGetRecordsThreadPool,
                config, shardSyncer, shardSyncStrategy);
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
            LOG.debug(message, t);
        }

        public void info(Object message) {
            if (this.isInfoEnabled()) {
                LOG.info(message);
            }
        }

        public void infoForce(Object message) {
            LOG.info(message);
        }

        @SuppressWarnings("unused")
        public void warn(Object message) {
            LOG.warn(message);
        }

        @SuppressWarnings("unused")
        public void error(Object message, Throwable t) {
            LOG.error(message, t);
        }

        private boolean isInfoEnabled() {
            return infoReporting;
        }

        private void resetInfoLogging() {
            if (infoReporting) {
                // We just logged at INFO level for a pass through worker loop
                if (LOG.isInfoEnabled()) {
                    infoReporting = false;
                    nextReportTime = System.currentTimeMillis() + reportIntervalMillis;
                } // else is DEBUG or TRACE so leave reporting true
            } else if (nextReportTime <= System.currentTimeMillis()) {
                infoReporting = true;
            }
        }
    }

    @VisibleForTesting
    StreamConfig getStreamConfig() {
        return streamConfig;
    }

    /**
     * Given configuration, returns appropriate metrics factory.
     *
     * @param cloudWatchClient
     *            Amazon CloudWatch client
     * @param config
     *            KinesisClientLibConfiguration
     * @return Returns metrics factory based on the config.
     */
    private static IMetricsFactory getMetricsFactory(AmazonCloudWatch cloudWatchClient,
            KinesisClientLibConfiguration config) {
        IMetricsFactory metricsFactory;
        if (config.getMetricsLevel() == MetricsLevel.NONE) {
            metricsFactory = new NullMetricsFactory();
        } else {
            if (config.getRegionName() != null) {
                setField(cloudWatchClient, "region", cloudWatchClient::setRegion, RegionUtils.getRegion(config.getRegionName()));
            }
            metricsFactory = new WorkerCWMetricsFactory(cloudWatchClient, config.getApplicationName(),
                    config.getMetricsBufferTimeMillis(), config.getMetricsMaxQueueSize(), config.getMetricsLevel(),
                    config.getMetricsEnabledDimensions());
        }
        return metricsFactory;
    }

    /**
     * Returns default executor service that should be used by the worker.
     *
     * @return Default executor service that should be used by the worker.
     */
    private static ExecutorService getExecutorService() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("RecordProcessor-%04d").build();
        return new WorkerThreadPoolExecutor(threadFactory);
    }

    private static <S, T> void setField(final S source, final String field, final Consumer<T> t, T value) {
        try {
            t.accept(value);
        } catch (UnsupportedOperationException e) {
            LOG.debug("Exception thrown while trying to set " + field + ", indicating that "
                    + source.getClass().getSimpleName() + "is immutable.", e);
        }
    }

    private PeriodicShardSyncStrategy createPeriodicShardSyncStrategy() {
        return new PeriodicShardSyncStrategy(leaderElectedPeriodicShardSyncManager);
    }

    private ShardEndShardSyncStrategy createShardEndShardSyncStrategy() {
        return new ShardEndShardSyncStrategy(shardSyncTaskManager, leaderElectedPeriodicShardSyncManager);
    }

    private LeaderDecider getOrCreateLeaderDecider(LeaderDecider leaderDecider) {
        if (leaderDecider != null) {
            return leaderDecider;
        }

        return new DeterministicShuffleShardSyncLeaderDecider(leaseCoordinator.getLeaseManager(),
                Executors.newSingleThreadScheduledExecutor(), PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT);
    }

    private PeriodicShardSyncManager getOrCreatePeriodicShardSyncManager(PeriodicShardSyncManager periodicShardSyncManager) {
        // TODO: Configure periodicShardSyncManager with either mandatory shard sync (PERIODIC) or hash range
        // validation based shard sync (SHARD_END) based on configured shard sync strategy
        if (periodicShardSyncManager != null) {
            return periodicShardSyncManager;
        }

        return new PeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(streamConfig.getStreamProxy(),
                        leaseCoordinator.getLeaseManager(),
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        SHARD_SYNC_SLEEP_FOR_PERIODIC_SHARD_SYNC,
                        shardSyncer,
                        null),
                metricsFactory);
    }

    /**
     * Extension to CWMetricsFactory, so worker can identify whether it owns the metrics factory instance or not.
     * Visible and non-final only for testing.
     */
    static class WorkerCWMetricsFactory extends CWMetricsFactory {

        WorkerCWMetricsFactory(AmazonCloudWatch cloudWatchClient, String namespace, long bufferTimeMillis,
                int maxQueueSize, MetricsLevel metricsLevel, Set<String> metricsEnabledDimensions) {
            super(cloudWatchClient, namespace, bufferTimeMillis, maxQueueSize, metricsLevel, metricsEnabledDimensions);
        }
    }

    /**
     * Extension to ThreadPoolExecutor, so worker can identify whether it owns the executor service instance or not.
     * Visible and non-final only for testing.
     */
    static class WorkerThreadPoolExecutor extends ThreadPoolExecutor {
        private static final long DEFAULT_KEEP_ALIVE_TIME = 60L;

        WorkerThreadPoolExecutor(ThreadFactory threadFactory) {
            // Defaults are based on Executors.newCachedThreadPool()
            super(0, Integer.MAX_VALUE, DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                    threadFactory);
        }
    }

    /**
     * Builder to construct a Worker instance.
     */
    public static class Builder {

        private IRecordProcessorFactory recordProcessorFactory;
        @Setter @Accessors(fluent = true)
        private KinesisClientLibConfiguration config;
        @Setter @Accessors(fluent = true)
        private AmazonKinesis kinesisClient;
        @Setter @Accessors(fluent = true)
        private AmazonDynamoDB dynamoDBClient;
        @Setter @Accessors(fluent = true)
        private AmazonCloudWatch cloudWatchClient;
        @Setter @Accessors(fluent = true)
        private IMetricsFactory metricsFactory;
        @Setter @Accessors(fluent = true)
        private ILeaseManager<KinesisClientLease> leaseManager;
        @Setter @Accessors(fluent = true)
        private ExecutorService execService;
        @Setter @Accessors(fluent = true)
        private ShardPrioritization shardPrioritization;
        @Setter @Accessors(fluent = true)
        private IKinesisProxy kinesisProxy;
        @Setter @Accessors(fluent = true)
        private WorkerStateChangeListener workerStateChangeListener;
        @Setter @Accessors(fluent = true)
        private LeaseCleanupValidator leaseCleanupValidator;
        @Setter @Accessors(fluent = true)
        private LeaseSelector<KinesisClientLease> leaseSelector;
        @Setter @Accessors(fluent = true)
        private LeaderDecider leaderDecider;
        @Setter @Accessors(fluent = true)
        private ILeaseTaker<KinesisClientLease> leaseTaker;
        @Setter @Accessors(fluent = true)
        private ILeaseRenewer<KinesisClientLease> leaseRenewer;
        @Setter @Accessors(fluent = true)
        private ShardSyncer shardSyncer;

        @VisibleForTesting
        AmazonKinesis getKinesisClient() {
            return kinesisClient;
        }

        @VisibleForTesting
        AmazonDynamoDB getDynamoDBClient() {
            return dynamoDBClient;
        }

        @VisibleForTesting
        AmazonCloudWatch getCloudWatchClient() {
            return cloudWatchClient;
        }

        /**
         * Provide a V1 {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
         * IRecordProcessor}.
         *
         * @param recordProcessorFactory
         *            Used to get record processor instances for processing data from shards
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder recordProcessorFactory(
                com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory) {
            this.recordProcessorFactory = new V1ToV2RecordProcessorFactoryAdapter(recordProcessorFactory);
            return this;
        }

        /**
         * Provide a V2 {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
         * IRecordProcessor}.
         *
         * @param recordProcessorFactory
         *            Used to get record processor instances for processing data from shards
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder recordProcessorFactory(IRecordProcessorFactory recordProcessorFactory) {
            this.recordProcessorFactory = recordProcessorFactory;
            return this;
        }

        /**
         * Build the Worker instance.
         *
         * @return a Worker instance.
         */
        // CHECKSTYLE:OFF CyclomaticComplexity
        // CHECKSTYLE:OFF NPathComplexity
        public Worker build() {
            if (config == null) {
                throw new IllegalArgumentException(
                        "Kinesis Client Library configuration needs to be provided to build Worker");
            }
            if (recordProcessorFactory == null) {
                throw new IllegalArgumentException("A Record Processor Factory needs to be provided to build Worker");
            }

            if (execService == null) {
                execService = getExecutorService();
            }
            if (kinesisClient == null) {
                kinesisClient = createClient(AmazonKinesisClientBuilder.standard(),
                        config.getKinesisCredentialsProvider(),
                        config.getKinesisClientConfiguration(),
                        config.getKinesisEndpoint(),
                        config.getRegionName());
            }
            if (dynamoDBClient == null) {
                dynamoDBClient = createClient(AmazonDynamoDBClientBuilder.standard(),
                        config.getDynamoDBCredentialsProvider(),
                        config.getDynamoDBClientConfiguration(),
                        config.getDynamoDBEndpoint(),
                        config.getRegionName());
            }
            if (cloudWatchClient == null) {
                cloudWatchClient = createClient(AmazonCloudWatchClientBuilder.standard(),
                        config.getCloudWatchCredentialsProvider(),
                        config.getCloudWatchClientConfiguration(),
                        null,
                        config.getRegionName());
            }
            // If a region name was explicitly specified, use it as the region for Amazon Kinesis and Amazon DynamoDB.
            if (config.getRegionName() != null) {
                setField(cloudWatchClient, "region", cloudWatchClient::setRegion, RegionUtils.getRegion(config.getRegionName()));
                setField(kinesisClient, "region", kinesisClient::setRegion, RegionUtils.getRegion(config.getRegionName()));
                setField(dynamoDBClient, "region", dynamoDBClient::setRegion, RegionUtils.getRegion(config.getRegionName()));
            }
            // If a dynamoDB endpoint was explicitly specified, use it to set the DynamoDB endpoint.
            if (config.getDynamoDBEndpoint() != null) {
                setField(dynamoDBClient, "endpoint", dynamoDBClient::setEndpoint, config.getDynamoDBEndpoint());
            }
            // If a kinesis endpoint was explicitly specified, use it to set the region of kinesis.
            if (config.getKinesisEndpoint() != null) {
                setField(kinesisClient, "endpoint", kinesisClient::setEndpoint, config.getKinesisEndpoint());
            }

            if (metricsFactory == null) {
                metricsFactory = getMetricsFactory(cloudWatchClient, config);
            }

            if (leaseManager == null) {
                leaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());
            }

            if (shardPrioritization == null) {
                shardPrioritization = new ParentsFirstShardPrioritization(1);
            }

            if (kinesisProxy == null) {
                kinesisProxy = new KinesisProxy(config, kinesisClient);
            }

            if (workerStateChangeListener == null) {
                workerStateChangeListener = DEFAULT_WORKER_STATE_CHANGE_LISTENER;
            }

            if (leaseCleanupValidator == null) {
                leaseCleanupValidator = DEFAULT_LEASE_CLEANUP_VALIDATOR;
            }

            if (shardSyncer == null) {
                shardSyncer = new KinesisShardSyncer(leaseCleanupValidator);
            }

            if(leaseSelector == null) {
                leaseSelector = DEFAULT_LEASE_SELECTOR;
            }

            if (leaseTaker == null) {
                leaseTaker = new LeaseTaker<>(leaseManager, leaseSelector, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                        .withMaxLeasesForWorker(config.getMaxLeasesForWorker())
                        .withMaxLeasesToStealAtOneTime(config.getMaxLeasesToStealAtOneTime());
            }

            // We expect users to either inject both LeaseRenewer and the corresponding thread-pool, or neither of them (DEFAULT).
           if (leaseRenewer == null) {
                ExecutorService leaseRenewerThreadPool = LeaseCoordinator.getDefaultLeaseRenewalExecutorService(config.getMaxLeaseRenewalThreads());
                leaseRenewer = new LeaseRenewer<>(leaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis(), leaseRenewerThreadPool);
            }

            if (leaderDecider == null) {
                leaderDecider = new DeterministicShuffleShardSyncLeaderDecider(leaseManager,
                    Executors.newSingleThreadScheduledExecutor(), PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT);
            }

            return new Worker(config.getApplicationName(),
                    recordProcessorFactory,
                    config,
                    new StreamConfig(kinesisProxy,
                            config.getMaxRecords(),
                            config.getIdleTimeBetweenReadsInMillis(),
                            config.shouldCallProcessRecordsEvenForEmptyRecordList(),
                            config.shouldValidateSequenceNumberBeforeCheckpointing(),
                            config.getInitialPositionInStreamExtended()),
                    config.getInitialPositionInStreamExtended(),
                    config.getParentShardPollIntervalMillis(),
                    config.getShardSyncIntervalMillis(),
                    config.shouldCleanupLeasesUponShardCompletion(),
                    null,
                    new KinesisClientLibLeaseCoordinator(leaseManager, leaseTaker, leaseRenewer,
                            config.getFailoverTimeMillis(),
                            config.getEpsilonMillis(),
                            config.getMaxLeasesForWorker(),
                            config.getMaxLeasesToStealAtOneTime(),
                            metricsFactory)
                            .withInitialLeaseTableReadCapacity(config.getInitialLeaseTableReadCapacity())
                            .withInitialLeaseTableWriteCapacity(config.getInitialLeaseTableWriteCapacity()),
                    execService,
                    metricsFactory,
                    config.getTaskBackoffTimeMillis(),
                    config.getFailoverTimeMillis(),
                    config.getSkipShardSyncAtWorkerInitializationIfLeasesExist(),
                    shardPrioritization,
                    config.getRetryGetRecordsInSeconds(),
                    config.getMaxGetRecordsThreadPool(),
                    workerStateChangeListener,
                    shardSyncer,
                    leaderDecider,
                    null /* PeriodicShardSyncManager */);
        }

        <R, T extends AwsClientBuilder<T, R>> R createClient(final T builder,
                final AWSCredentialsProvider credentialsProvider,
                final ClientConfiguration clientConfiguration,
                final String endpointUrl,
                final String region) {
            if (credentialsProvider != null) {
                builder.withCredentials(credentialsProvider);
            }
            if (clientConfiguration != null) {
                builder.withClientConfiguration(clientConfiguration);
            }
            if (StringUtils.isNotEmpty(endpointUrl)) {
                LOG.warn("Received configuration for endpoint as " + endpointUrl + ", and region as "
                        + region + ".");
                builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointUrl, region));
            } else if (StringUtils.isNotEmpty(region)) {
                LOG.warn("Received configuration for region as " + region + ".");
                builder.withRegion(region);
            } else {
                LOG.warn("No configuration received for endpoint and region, will default region to us-east-1");
                builder.withRegion(Regions.US_EAST_1);
            }
            return builder.build();
        }
    }
}
