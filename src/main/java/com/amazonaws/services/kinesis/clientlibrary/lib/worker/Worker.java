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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxyFactory;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

/**
 * Worker is the high level class that Kinesis applications use to start processing data.
 * It initializes and oversees different components (e.g. syncing shard and lease information, tracking shard
 * assignments, and processing data from the shards).
 */
public class Worker implements Runnable {

    private static final int MAX_INITIALIZATION_ATTEMPTS = 20;
    private static final Log LOG = LogFactory.getLog(Worker.class);
    private WorkerLog wlog = new WorkerLog();

    private final String applicationName;
    private final IRecordProcessorFactory recordProcessorFactory;
    private final StreamConfig streamConfig;
    private final InitialPositionInStream initialPosition;
    private final ICheckpoint checkpointTracker;
    private final long idleTimeInMilliseconds;
    // Backoff time when polling to check if application has finished processing parent shards
    private final long parentShardPollIntervalMillis;
    private final ExecutorService executorService;
    private final IMetricsFactory metricsFactory;
    // Backoff time when running tasks if they encounter exceptions
    private final long taskBackoffTimeMillis;

    // private final KinesisClientLeaseManager leaseManager;
    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private final ShardSyncTaskManager controlServer;

    private boolean shutdown;

    // Holds consumers for shards the worker is currently tracking. Key is shard id, value is ShardConsumer.
    private ConcurrentMap<String, ShardConsumer> shardIdShardConsumerMap =
            new ConcurrentHashMap<String, ShardConsumer>();
    private final boolean cleanupLeasesUponShardCompletion;

    /**
     * Constructor.
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     */
    public Worker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config) {
        this(recordProcessorFactory, config, Executors.newCachedThreadPool());
    }

    /**
     * Constructor.
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(IRecordProcessorFactory recordProcessorFactory, 
            KinesisClientLibConfiguration config, ExecutorService execService) {
        this(recordProcessorFactory, config,
            new AmazonKinesisClient(config.getKinesisCredentialsProvider(), config.getKinesisClientConfiguration()),
            new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(), config.getDynamoDBClientConfiguration()),
            new AmazonCloudWatchClient(config.getCloudWatchCredentialsProvider(), 
                    config.getCloudWatchClientConfiguration()),
            execService);
    }
    
    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param metricsFactory Metrics factory used to emit metrics
     */
    public Worker(IRecordProcessorFactory recordProcessorFactory, 
            KinesisClientLibConfiguration config,
            IMetricsFactory metricsFactory) {
        this(recordProcessorFactory, config, metricsFactory, Executors.newCachedThreadPool());
    }
    
    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param metricsFactory Metrics factory used to emit metrics
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(IRecordProcessorFactory recordProcessorFactory, 
            KinesisClientLibConfiguration config,
            IMetricsFactory metricsFactory,
            ExecutorService execService) {
        this(recordProcessorFactory, config,
            new AmazonKinesisClient(config.getKinesisCredentialsProvider(), config.getKinesisClientConfiguration()),
            new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(), config.getDynamoDBClientConfiguration()),
            metricsFactory,
            execService);
    }
    
    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param kinesisClient Kinesis Client used for fetching data
     * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient CloudWatch Client for publishing metrics
     */
    public Worker(IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient,
            AmazonCloudWatchClient cloudWatchClient) {
        this(recordProcessorFactory, config, 
                kinesisClient, dynamoDBClient, cloudWatchClient,
                Executors.newCachedThreadPool());
    }
    
    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param kinesisClient Kinesis Client used for fetching data
     * @param dynamoDBClient DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient Clould Watch Client for using cloud watch
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    public Worker(IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient,
            AmazonCloudWatchClient cloudWatchClient,
            ExecutorService execService) {
        this(recordProcessorFactory, config, 
                kinesisClient, dynamoDBClient,
                new CWMetricsFactory(
                        cloudWatchClient,
                        config.getApplicationName(),
                        config.getMetricsBufferTimeMillis(),
                        config.getMetricsMaxQueueSize()),
                execService);
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
    public Worker(IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            AmazonKinesisClient kinesisClient,
            AmazonDynamoDBClient dynamoDBClient,
            IMetricsFactory metricsFactory,
            ExecutorService execService) {
        this(recordProcessorFactory, config, 
                new StreamConfig(
                        new KinesisProxyFactory(config.getKinesisCredentialsProvider(), 
                                kinesisClient).getProxy(config.getStreamName()),
                        config.getMaxRecords(),
                        config.getIdleTimeBetweenReadsInMillis(),
                        config.shouldCallProcessRecordsEvenForEmptyRecordList()),
                new KinesisClientLibLeaseCoordinator(
                        new KinesisClientLeaseManager(config.getApplicationName(), dynamoDBClient),
                        config.getWorkerIdentifier(),
                        config.getFailoverTimeMillis(),
                        config.getEpsilonMillis(),
                        metricsFactory), 
                metricsFactory, execService);
        // If an endpoint was explicitly specified, use it.
        if (config.getKinesisEndpoint() != null) {
            kinesisClient.setEndpoint(config.getKinesisEndpoint());
        }
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config Kinesis Client Library configuration
     * @param streamConfig Stream configuration
     * @param leaseCoordinator Lease coordinator (coordinates currently owned leases and checkpoints)
     * @param metricsFactory Metrics factory used to emit metrics
     * @param execService ExecutorService to use for processing records (support for multi-threaded
     *        consumption)
     */
    private Worker(IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration config,
            StreamConfig streamConfig,
            KinesisClientLibLeaseCoordinator leaseCoordinator,
            IMetricsFactory metricsFactory,
            ExecutorService execService) {
        this(config.getApplicationName(), recordProcessorFactory, streamConfig, config.getInitialPositionInStream(),
                config.getParentShardPollIntervalMillis(), config.getShardSyncIntervalMillis(),
                config.shouldCleanupLeasesUponShardCompletion(), leaseCoordinator, leaseCoordinator, execService,
                metricsFactory, config.getTaskBackoffTimeMillis());
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
            long taskBackoffTimeMillis) {
        this.applicationName = applicationName;
        this.recordProcessorFactory = recordProcessorFactory;
        this.streamConfig = streamConfig;
        this.initialPosition = initialPositionInStream;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.checkpointTracker = checkpoint;
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
    }

    /**
     * @return the applicationName
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Start consuming data from the stream, and pass it to the application record processors.
     */
    public void run() {
        try {
            initialize();
            LOG.info("Initialization complete. Starting worker loop.");
        } catch (RuntimeException e1) {
            LOG.error("Unable to initialize after " + MAX_INITIALIZATION_ATTEMPTS + " attempts. Shutting down.", e1);
            shutdown();
        }

        while (!shutdown) {
            try {
                boolean foundCompletedShard = false;
                Set<String> assignedShardIds = new HashSet<String>();
                for (ShardInfo shardInfo : getShardInfoForAssignments()) {
                    ShardConsumer shardConsumer = createOrGetShardConsumer(shardInfo, recordProcessorFactory);
                    if (shardConsumer.isShutdown()
                            && shardConsumer.getShutdownReason().equals(ShutdownReason.TERMINATE)) {
                        foundCompletedShard = true;
                    } else {
                        shardConsumer.consumeShard();
                    }
                    assignedShardIds.add(shardInfo.getShardId());
                }

                if (foundCompletedShard) {
                    controlServer.syncShardAndLeaseInfo(null);
                }

                // clean up shard consumers for unassigned shards
                cleanupShardConsumers(assignedShardIds);

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

        LOG.info("Stopping LeaseCoordinator.");
        leaseCoordinator.stop();
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

    private void cleanupShardConsumers(Set<String> assignedShardIds) {
        for (String shardId : shardIdShardConsumerMap.keySet()) {
            if (!assignedShardIds.contains(shardId)) {
                // Shutdown the consumer since we are not longer responsible for the shard.
                boolean isShutdown = shardIdShardConsumerMap.get(shardId).beginShutdown();
                if (isShutdown) {
                    shardIdShardConsumerMap.remove(shardId);
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
     * Sets the killed flag so this worker will stop on the next iteration of its loop.
     */
    public void shutdown() {
        this.shutdown = true;
    }

    /**
     * NOTE: This method is internal/private to the Worker class. It has package access solely for
     * testing.
     * 
     * @param shardInfo Kinesis shard info
     * @param factory RecordProcessor factory
     * @return ShardConsumer for the shard
     */
    ShardConsumer createOrGetShardConsumer(ShardInfo shardInfo, IRecordProcessorFactory factory) {
        synchronized (shardIdShardConsumerMap) {
            String shardId = shardInfo.getShardId();
            ShardConsumer consumer = shardIdShardConsumerMap.get(shardId);
            // Instantiate a new consumer if we don't have one, or the one we had was from an earlier
            // lease instance (and was shutdown). Don't need to create another one if the shard has been
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
                shardIdShardConsumerMap.put(shardId, consumer);
                wlog.infoForce("Created new shardConsumer for shardId: " + shardId + ", concurrencyToken: "
                        + shardInfo.getConcurrencyToken());
            }
            return consumer;
        }
    }

    /**
     * Logger for suppressing too much INFO logging.
     * To avoid too much logging information Worker will output logging at INFO level
     * for a single pass through the main loop every minute.
     * At DEBUG level it will output all INFO logs on every pass.
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
}
