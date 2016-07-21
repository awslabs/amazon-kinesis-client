/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxyFactory;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

/**
 * Worker is the high level class that Kinesis applications use to start
 * processing data. It initializes and oversees different components (e.g.
 * syncing shard and lease information, tracking shard assignments, and
 * processing data from the shards).
 */
public class Worker implements Runnable {

    private static final Log LOG = LogFactory.getLog(Worker.class);

    private static final int MAX_INITIALIZATION_ATTEMPTS = 20;

    private WorkerLog wlog = new WorkerLog();

    private final String applicationName;
    private final IRecordProcessorFactory recordProcessorFactory;
    private final StreamConfig streamConfig;
    private final InitialPositionInStream initialPosition;
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

    // private final KinesisClientLeaseManager leaseManager;
    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private final ShardSyncTaskManager controlServer;

    private volatile boolean shutdown;
    private volatile long shutdownStartTimeMillis;

    // Holds consumers for shards the worker is currently tracking. Key is shard
    // info, value is ShardConsumer.
    private ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap =
            new ConcurrentHashMap<ShardInfo, ShardConsumer>();
    private final boolean cleanupLeasesUponShardCompletion;

    /**
     * Constructor.
     *
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config) {
        this(recordProcessorFactory, config, getExecutorService());
    }

    /**
     * Constructor.
     *
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            ExecutorService execService) {
        this(recordProcessorFactory, config, new AmazonKinesisClient(config.getKinesisCredentialsProvider(),
                config.getKinesisClientConfiguration()),
                new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(),
                        config.getDynamoDBClientConfiguration()),
                new AmazonCloudWatchClient(config.getCloudWatchCredentialsProvider(),
                        config.getCloudWatchClientConfiguration()), execService);
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param metricsFactory Metrics factory used to emit metrics
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            IMetricsFactory metricsFactory) {
        this(recordProcessorFactory, config, metricsFactory, getExecutorService());
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param metricsFactory Metrics factory used to emit metrics
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            IMetricsFactory metricsFactory,
            ExecutorService execService) {
        this(recordProcessorFactory, config, new AmazonKinesisClient(config.getKinesisCredentialsProvider(),
                config.getKinesisClientConfiguration()),
                new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(),
                        config.getDynamoDBClientConfiguration()), metricsFactory, execService);
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param kinesisClient Kinesis Client used for fetching data
     * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient CloudWatch Client for publishing metrics
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesis kinesisClient,
            AmazonDynamoDB dynamoDBClient,
            AmazonCloudWatch cloudWatchClient) {
        this(recordProcessorFactory, config, kinesisClient, dynamoDBClient, cloudWatchClient, getExecutorService());
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param kinesisClient Kinesis Client used for fetching data
     * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient CloudWatch Client for publishing metrics
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesis kinesisClient,
            AmazonDynamoDB dynamoDBClient,
            AmazonCloudWatch cloudWatchClient,
            ExecutorService execService) {
        this(recordProcessorFactory, config, kinesisClient, dynamoDBClient,
                getMetricsFactory(cloudWatchClient, config), execService);
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param kinesisClient Kinesis Client used for fetching data
     * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory Metrics factory used to emit metrics
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesis kinesisClient,
            AmazonDynamoDB dynamoDBClient,
            IMetricsFactory metricsFactory,
            ExecutorService execService) {
        this(
                config.getApplicationName(),
                new V1ToV2RecordProcessorFactoryAdapter(recordProcessorFactory),
                new StreamConfig(
                        new KinesisProxyFactory(config.getKinesisCredentialsProvider(), kinesisClient)
                            .getProxy(config.getStreamName()),
                        config.getMaxRecords(), config.getIdleTimeBetweenReadsInMillis(),
                        config.shouldCallProcessRecordsEvenForEmptyRecordList(),
                        config.shouldValidateSequenceNumberBeforeCheckpointing(),
                        config.getInitialPositionInStream()),
                config.getInitialPositionInStream(),
                config.getParentShardPollIntervalMillis(),
                config.getShardSyncIntervalMillis(),
                config.shouldCleanupLeasesUponShardCompletion(),
                null,
                new KinesisClientLibLeaseCoordinator(
                        new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient),
                        config.getWorkerIdentifier(),
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
                config.getFailoverTimeMillis());
        // If a region name was explicitly specified, use it as the region for Amazon Kinesis and Amazon DynamoDB.
        if (config.getRegionName() != null) {
            Region region = RegionUtils.getRegion(config.getRegionName());
            kinesisClient.setRegion(region);
            LOG.debug("The region of Amazon Kinesis client has been set to " + config.getRegionName());
            dynamoDBClient.setRegion(region);
            LOG.debug("The region of Amazon DynamoDB client has been set to " + config.getRegionName());
        }
        // If a kinesis endpoint was explicitly specified, use it to set the region of kinesis.
        if (config.getKinesisEndpoint() != null) {
            kinesisClient.setEndpoint(config.getKinesisEndpoint());
            if (config.getRegionName() != null) {
                LOG.warn("Received configuration for both region name as " + config.getRegionName()
                        + ", and Amazon Kinesis endpoint as " + config.getKinesisEndpoint()
                        + ". Amazon Kinesis endpoint will overwrite region name.");
                LOG.debug("The region of Amazon Kinesis client has been overwritten to " + config.getKinesisEndpoint());
            } else  {
                LOG.debug("The region of Amazon Kinesis client has been set to " + config.getKinesisEndpoint());
            }
        }
    }

    /**
     * @param applicationName Name of the Kinesis application
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param streamConfig Stream configuration
     * @param initialPositionInStream One of LATEST or TRIM_HORIZON. The KinesisClientLibrary will start fetching data
     *        from this location in the stream when an application starts up for the first time and there are no
     *        checkpoints. If there are checkpoints, we start from the checkpoint position.
     * @param parentShardPollIntervalMillis Wait for this long between polls to check if parent shards are done
     * @param shardSyncIdleTimeMillis Time between tasks to sync leases and Kinesis shards
     * @param cleanupLeasesUponShardCompletion Clean up shards we've finished processing (don't wait till they expire in
     *        Kinesis)
     * @param checkpoint Used to get/set checkpoints
     * @param leaseCoordinator Lease coordinator (coordinates currently owned leases)
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     * @param metricsFactory Metrics factory used to emit metrics
     * @param taskBackoffTimeMillis Backoff period when tasks encounter an exception
     */
    // NOTE: This has package level access solely for testing
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    Worker(String applicationName,
            IRecordProcessorFactory recordProcessorFactory,
            StreamConfig streamConfig,
            InitialPositionInStream initialPositionInStream,
            long parentShardPollIntervalMillis,
            long shardSyncIdleTimeMillis,
            boolean cleanupLeasesUponShardCompletion,
            ICheckpoint checkpoint,
            KinesisClientLibLeaseCoordinator leaseCoordinator,
            ExecutorService execService,
            IMetricsFactory metricsFactory,
            long taskBackoffTimeMillis,
            long failoverTimeMillis) {
        this.applicationName = applicationName;
        this.recordProcessorFactory = recordProcessorFactory;
        this.streamConfig = streamConfig;
        this.initialPosition = initialPositionInStream;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.checkpointTracker = checkpoint != null ? checkpoint : leaseCoordinator;
        this.idleTimeInMilliseconds = streamConfig.getIdleTimeInMilliseconds();
        this.executorService = execService;
        this.leaseCoordinator = leaseCoordinator;
        this.metricsFactory = metricsFactory;
        this.controlServer =
                new ShardSyncTaskManager(streamConfig.getStreamProxy(),
                        leaseCoordinator.getLeaseManager(),
                        initialPositionInStream,
                        cleanupLeasesUponShardCompletion,
                        shardSyncIdleTimeMillis,
                        metricsFactory,
                        executorService);
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
        this.failoverTimeMillis = failoverTimeMillis;
    }

    /**
     * @return the applicationName
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Start consuming data from the stream, and pass it to the application
     * record processors.
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
            try {
                boolean foundCompletedShard = false;
                Set<ShardInfo> assignedShards = new HashSet<ShardInfo>();
                for (ShardInfo shardInfo : getShardInfoForAssignments()) {
                    ShardConsumer shardConsumer = createOrGetShardConsumer(shardInfo, recordProcessorFactory);
                    if (shardConsumer.isShutdown()
                            && shardConsumer.getShutdownReason().equals(ShutdownReason.TERMINATE)) {
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
                LOG.error(String.format("Worker.run caught exception, sleeping for %s milli seconds!",
                        String.valueOf(idleTimeInMilliseconds)),
                        e);
                try {
                    Thread.sleep(idleTimeInMilliseconds);
                } catch (InterruptedException ex) {
                    LOG.info("Worker: sleep interrupted after catching exception ", ex);
                }
            }
            wlog.resetInfoLogging();
        }

        finalShutdown();
        LOG.info("Worker loop is complete. Exiting from worker.");
    }

    private void initialize() {
        boolean isDone = false;
        Exception lastException = null;

        for (int i = 0; (!isDone) && (i < MAX_INITIALIZATION_ATTEMPTS); i++) {
            try {
                LOG.info("Initialization attempt " + (i + 1));
                LOG.info("Initializing LeaseCoordinator");
                leaseCoordinator.initialize();

                LOG.info("Syncing Kinesis shard info");
                ShardSyncTask shardSyncTask =
                        new ShardSyncTask(streamConfig.getStreamProxy(),
                                leaseCoordinator.getLeaseManager(),
                                initialPosition,
                                cleanupLeasesUponShardCompletion,
                                0L);
                TaskResult result = new MetricsCollectingTaskDecorator(shardSyncTask, metricsFactory).call();

                if (result.getException() == null) {
                    if (!leaseCoordinator.isRunning()) {
                        LOG.info("Starting LeaseCoordinator");
                        leaseCoordinator.start();
                    } else {
                        LOG.info("LeaseCoordinator is already running. No need to start it.");
                    }
                    isDone = true;
                } else {
                    lastException = result.getException();
                }
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
            throw new RuntimeException(lastException);
        }
    }

    /**
     * NOTE: This method is internal/private to the Worker class. It has package
     * access solely for testing.
     *
     * This method relies on ShardInfo.equals() method returning true for ShardInfo objects which may have been
     * instantiated with parentShardIds in a different order (and rest of the fields being the equal). For example
     * shardInfo1.equals(shardInfo2) should return true with shardInfo1 and shardInfo2 defined as follows.
     * ShardInfo shardInfo1 = new ShardInfo(shardId1, concurrencyToken1, Arrays.asList("parent1", "parent2"));
     * ShardInfo shardInfo2 = new ShardInfo(shardId1, concurrencyToken1, Arrays.asList("parent2", "parent1"));
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

        if ((assignedStreamShards != null) && (!assignedStreamShards.isEmpty())) {
            if (wlog.isInfoEnabled()) {
                StringBuilder builder = new StringBuilder();
                boolean firstItem = true;
                for (ShardInfo shardInfo : assignedStreamShards) {
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

        return assignedStreamShards;
    }

    /**
     * Signals worker to shutdown. Worker will try initiating shutdown of all record processors. Note that
     * if executor services were passed to the worker by the user, worker will not attempt to shutdown
     * those resources.
     */
    public void shutdown() {
        LOG.info("Worker shutdown requested.");

        // Set shutdown flag, so Worker.run can start shutdown process.
        shutdown = true;
        shutdownStartTimeMillis = System.currentTimeMillis();

        // Stop lease coordinator, so leases are not renewed or stolen from other workers.
        // Lost leases will force Worker to begin shutdown process for all shard consumers in
        // Worker.run().
        leaseCoordinator.stop();
    }

    /**
     * Perform final shutdown related tasks for the worker including shutting down worker owned
     * executor services, threads, etc.
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
    }

    /**
     * Returns whether worker can shutdown immediately. Note that this method is called from Worker's {{@link #run()}
     * method before every loop run, so method must do minimum amount of work to not impact shard processing timings.
     * @return Whether worker should shutdown immediately.
     */
    private boolean shouldShutdown() {
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
     * NOTE: This method is internal/private to the Worker class. It has package
     * access solely for testing.
     *
     * @param shardInfo Kinesis shard info
     * @param factory RecordProcessor factory
     * @return ShardConsumer for the shard
     */
    ShardConsumer createOrGetShardConsumer(ShardInfo shardInfo, IRecordProcessorFactory factory) {
        ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);
        // Instantiate a new consumer if we don't have one, or the one we
        // had was from an earlier
        // lease instance (and was shutdown). Don't need to create another
        // one if the shard has been
        // completely processed (shutdown reason terminate).
        if ((consumer == null)
                || (consumer.isShutdown() && consumer.getShutdownReason().equals(ShutdownReason.ZOMBIE))) {
            IRecordProcessor recordProcessor = factory.createProcessor();

            consumer =
                    new ShardConsumer(shardInfo,
                           streamConfig,
                           checkpointTracker,
                           recordProcessor,
                           leaseCoordinator.getLeaseManager(),
                           parentShardPollIntervalMillis,
                           cleanupLeasesUponShardCompletion,
                           executorService,
                           metricsFactory,
                           taskBackoffTimeMillis);
            shardInfoShardConsumerMap.put(shardInfo, consumer);
            wlog.infoForce("Created new shardConsumer for : " + shardInfo);
        }
        return consumer;
    }

    /**
     * Logger for suppressing too much INFO logging. To avoid too much logging
     * information Worker will output logging at INFO level for a single pass
     * through the main loop every minute. At DEBUG level it will output all
     * INFO logs on every pass.
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

    // Backwards compatible constructors
    /**
     * This constructor is for binary compatibility with code compiled against
     * version of the KCL that only have constructors taking "Client" objects.
     *
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param kinesisClient Kinesis Client used for fetching data
     * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient CloudWatch Client for publishing metrics
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient,
            AmazonCloudWatchClient cloudWatchClient) {
        this(recordProcessorFactory,
                 config,
                (AmazonKinesis) kinesisClient,
                (AmazonDynamoDB) dynamoDBClient,
                (AmazonCloudWatch) cloudWatchClient);
    }

    /**
     * This constructor is for binary compatibility with code compiled against
     * version of the KCL that only have constructors taking "Client" objects.
     *
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param kinesisClient Kinesis Client used for fetching data
     * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient CloudWatch Client for publishing metrics
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient,
            AmazonCloudWatchClient cloudWatchClient,
            ExecutorService execService) {
        this(recordProcessorFactory,
                 config,
                 (AmazonKinesis) kinesisClient,
                 (AmazonDynamoDB) dynamoDBClient,
                 (AmazonCloudWatch) cloudWatchClient,
                 execService);
    }

    /**
     * This constructor is for binary compatibility with code compiled against
     * version of the KCL that only have constructors taking "Client" objects.
     *
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param kinesisClient Kinesis Client used for fetching data
     * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory Metrics factory used to emit metrics
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient,
            IMetricsFactory metricsFactory,
            ExecutorService execService) {
        this(recordProcessorFactory,
                 config,
                (AmazonKinesis) kinesisClient,
                (AmazonDynamoDB) dynamoDBClient,
                 metricsFactory,
                 execService);
    }

    /**
     * Given configuration, returns appropriate metrics factory.
     * @param cloudWatchClient Amazon CloudWatch client
     * @param config KinesisClientLibConfiguration
     * @return Returns metrics factory based on the config.
     */
    private static IMetricsFactory getMetricsFactory(
            AmazonCloudWatch cloudWatchClient, KinesisClientLibConfiguration config) {
        IMetricsFactory metricsFactory;
        if (config.getMetricsLevel() == MetricsLevel.NONE) {
            metricsFactory = new NullMetricsFactory();
        } else {
            if (config.getRegionName() != null) {
                Region region = RegionUtils.getRegion(config.getRegionName());
                cloudWatchClient.setRegion(region);
                LOG.debug("The region of Amazon CloudWatch client has been set to " + config.getRegionName());
            }
            metricsFactory = new WorkerCWMetricsFactory(
                    cloudWatchClient,
                    config.getApplicationName(),
                    config.getMetricsBufferTimeMillis(),
                    config.getMetricsMaxQueueSize(),
                    config.getMetricsLevel(),
                    config.getMetricsEnabledDimensions());
        }
        return metricsFactory;
    }

    /**
     * Returns default executor service that should be used by the worker.
     * @return Default executor service that should be used by the worker.
     */
    private static ExecutorService getExecutorService() {
        return new WorkerThreadPoolExecutor();
    }

    /**
     * Extension to CWMetricsFactory, so worker can identify whether it owns the metrics factory instance
     * or not.
     * Visible and non-final only for testing.
     */
    static class WorkerCWMetricsFactory extends CWMetricsFactory {

        WorkerCWMetricsFactory(AmazonCloudWatch cloudWatchClient,
                String namespace,
                long bufferTimeMillis,
                int maxQueueSize,
                MetricsLevel metricsLevel,
                Set<String> metricsEnabledDimensions) {
            super(cloudWatchClient, namespace, bufferTimeMillis,
                    maxQueueSize, metricsLevel, metricsEnabledDimensions);
        }
    }

    /**
     * Extension to ThreadPoolExecutor, so worker can identify whether it owns the executor service instance
     * or not.
     * Visible and non-final only for testing.
     */
    static class WorkerThreadPoolExecutor extends ThreadPoolExecutor {
        private static final long DEFAULT_KEEP_ALIVE_TIME = 60L;

        WorkerThreadPoolExecutor() {
            // Defaults are based on Executors.newCachedThreadPool()
            super(0, Integer.MAX_VALUE, DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
        }
    }

    /**
     * Builder to construct a Worker instance.
     */
    public static class Builder {

        private IRecordProcessorFactory recordProcessorFactory;
        private KinesisClientLibConfiguration config;
        private AmazonKinesis kinesisClient;
        private AmazonDynamoDB dynamoDBClient;
        private AmazonCloudWatch cloudWatchClient;
        private IMetricsFactory metricsFactory;
        private ExecutorService execService;

        /**
         * Default constructor.
         */
        public Builder() {
        }

        /**
         * Provide a V1
         * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor IRecordProcessor}.
         *
         * @param recordProcessorFactory Used to get record processor instances for processing data from shards
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder recordProcessorFactory(
                com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
                recordProcessorFactory) {
            this.recordProcessorFactory = new V1ToV2RecordProcessorFactoryAdapter(recordProcessorFactory);
            return this;
        }

        /**
         * Provide a V2
         * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor IRecordProcessor}.
         *
         * @param recordProcessorFactory Used to get record processor instances for processing data from shards
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder recordProcessorFactory(IRecordProcessorFactory recordProcessorFactory) {
            this.recordProcessorFactory = recordProcessorFactory;
            return this;
        }

        /**
         * Set the Worker config.
         *
         * @param config Kinesis Client Library configuration
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder config(KinesisClientLibConfiguration config) {
            this.config = config;
            return this;
        }

        /**
         * Set the Kinesis client.
         *
         * @param kinesisClient Kinesis Client used for fetching data
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder kinesisClient(AmazonKinesis kinesisClient) {
            this.kinesisClient = kinesisClient;
            return this;
        }

        /**
         * Set the DynamoDB client.
         *
         * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder dynamoDBClient(AmazonDynamoDB dynamoDBClient) {
            this.dynamoDBClient = dynamoDBClient;
            return this;
        }

        /**
         * Set the Cloudwatch client.
         *
         * @param cloudWatchClient CloudWatch Client for publishing metrics
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder cloudWatchClient(AmazonCloudWatch cloudWatchClient) {
            this.cloudWatchClient = cloudWatchClient;
            return this;
        }

        /**
         * Set the metrics factory.
         *
         * @param metricsFactory Metrics factory used to emit metrics
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder metricsFactory(IMetricsFactory metricsFactory) {
            this.metricsFactory = metricsFactory;
            return this;
        }

        /**
         * Set the executor service for processing records.
         *
         * @param execService ExecutorService to use for processing records (support for multi-threaded consumption)
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public Builder execService(ExecutorService execService) {
            this.execService = execService;
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
                throw new IllegalArgumentException(
                        "A Record Processor Factory needs to be provided to build Worker");
            }

            if (execService == null) {
                execService = getExecutorService();
            }
            if (kinesisClient == null) {
                kinesisClient = new AmazonKinesisClient(config.getKinesisCredentialsProvider(),
                        config.getKinesisClientConfiguration());
            }
            if (dynamoDBClient == null) {
                dynamoDBClient = new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(),
                        config.getDynamoDBClientConfiguration());
            }
            if (cloudWatchClient == null) {
                cloudWatchClient = new AmazonCloudWatchClient(config.getCloudWatchCredentialsProvider(),
                        config.getCloudWatchClientConfiguration());
            }
            // If a region name was explicitly specified, use it as the region for Amazon Kinesis and Amazon DynamoDB.
            if (config.getRegionName() != null) {
                Region region = RegionUtils.getRegion(config.getRegionName());
                cloudWatchClient.setRegion(region);
                LOG.debug("The region of Amazon CloudWatch client has been set to " + config.getRegionName());
                kinesisClient.setRegion(region);
                LOG.debug("The region of Amazon Kinesis client has been set to " + config.getRegionName());
                dynamoDBClient.setRegion(region);
                LOG.debug("The region of Amazon DynamoDB client has been set to " + config.getRegionName());
            }
            // If a kinesis endpoint was explicitly specified, use it to set the region of kinesis.
            if (config.getKinesisEndpoint() != null) {
                kinesisClient.setEndpoint(config.getKinesisEndpoint());
                if (config.getRegionName() != null) {
                    LOG.warn("Received configuration for both region name as " + config.getRegionName()
                            + ", and Amazon Kinesis endpoint as " + config.getKinesisEndpoint()
                            + ". Amazon Kinesis endpoint will overwrite region name.");
                    LOG.debug("The region of Amazon Kinesis client has been overwritten to "
                            + config.getKinesisEndpoint());
                } else  {
                    LOG.debug("The region of Amazon Kinesis client has been set to " + config.getKinesisEndpoint());
                }
            }
            if (metricsFactory == null) {
                metricsFactory = getMetricsFactory(cloudWatchClient, config);
            }

            return new Worker(config.getApplicationName(),
                    recordProcessorFactory,
                    new StreamConfig(new KinesisProxyFactory(config.getKinesisCredentialsProvider(),
                            kinesisClient).getProxy(config.getStreamName()),
                            config.getMaxRecords(),
                            config.getIdleTimeBetweenReadsInMillis(),
                            config.shouldCallProcessRecordsEvenForEmptyRecordList(),
                            config.shouldValidateSequenceNumberBeforeCheckpointing(),
                            config.getInitialPositionInStream()),
                    config.getInitialPositionInStream(),
                    config.getParentShardPollIntervalMillis(),
                    config.getShardSyncIntervalMillis(),
                    config.shouldCleanupLeasesUponShardCompletion(),
                    null,
                    new KinesisClientLibLeaseCoordinator(new KinesisClientLeaseManager(config.getTableName(),
                            dynamoDBClient),
                            config.getWorkerIdentifier(),
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
                    config.getFailoverTimeMillis());
        }

    }
}
