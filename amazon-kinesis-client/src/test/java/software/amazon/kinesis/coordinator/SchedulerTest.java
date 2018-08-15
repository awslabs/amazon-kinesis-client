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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.checkpoint.CheckpointFactory;
import software.amazon.kinesis.exceptions.KinesisClientLibNonRetryableException;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseManagementFactory;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SchedulerTest {
    private final String tableName = "tableName";
    private final String workerIdentifier = "workerIdentifier";
    private final String applicationName = "applicationName";
    private final String streamName = "streamName";
    private final String namespace = "testNamespace";

    private Scheduler scheduler;
    private ShardRecordProcessorFactory shardRecordProcessorFactory;
    private CheckpointConfig checkpointConfig;
    private CoordinatorConfig coordinatorConfig;
    private LeaseManagementConfig leaseManagementConfig;
    private LifecycleConfig lifecycleConfig;
    private MetricsConfig metricsConfig;
    private ProcessorConfig processorConfig;
    private RetrievalConfig retrievalConfig;

    @Mock
    private KinesisAsyncClient kinesisClient;
    @Mock
    private DynamoDbAsyncClient dynamoDBClient;
    @Mock
    private CloudWatchAsyncClient cloudWatchClient;
    @Mock
    private RetrievalFactory retrievalFactory;
    @Mock
    private RecordsPublisher recordsPublisher;
    @Mock
    private LeaseCoordinator leaseCoordinator;
    @Mock
    private ShardSyncTaskManager shardSyncTaskManager;
    @Mock
    private DynamoDBLeaseRefresher dynamoDBLeaseRefresher;
    @Mock
    private ShardDetector shardDetector;
    @Mock
    private Checkpointer checkpoint;

    @Before
    public void setup() {
        shardRecordProcessorFactory = new TestShardRecordProcessorFactory();

        checkpointConfig = new CheckpointConfig().checkpointFactory(new TestKinesisCheckpointFactory());
        coordinatorConfig = new CoordinatorConfig(applicationName).parentShardPollIntervalMillis(100L);
        leaseManagementConfig = new LeaseManagementConfig(tableName, dynamoDBClient, kinesisClient, streamName,
                workerIdentifier).leaseManagementFactory(new TestKinesisLeaseManagementFactory());
        lifecycleConfig = new LifecycleConfig();
        metricsConfig = new MetricsConfig(cloudWatchClient, namespace);
        processorConfig = new ProcessorConfig(shardRecordProcessorFactory);
        retrievalConfig = new RetrievalConfig(kinesisClient, streamName, applicationName)
                .retrievalFactory(retrievalFactory);

        when(leaseCoordinator.leaseRefresher()).thenReturn(dynamoDBLeaseRefresher);
        when(shardSyncTaskManager.shardDetector()).thenReturn(shardDetector);
        when(retrievalFactory.createGetRecordsCache(any(ShardInfo.class), any(MetricsFactory.class))).thenReturn(recordsPublisher);

        scheduler = new Scheduler(checkpointConfig, coordinatorConfig, leaseManagementConfig, lifecycleConfig,
                metricsConfig, processorConfig, retrievalConfig);
    }

    /**
     * Test method for {@link Scheduler#applicationName()}.
     */
    @Test
    public void testGetStageName() {
        final String stageName = "testStageName";
        coordinatorConfig = new CoordinatorConfig(stageName);
        scheduler = new Scheduler(checkpointConfig, coordinatorConfig, leaseManagementConfig, lifecycleConfig,
                metricsConfig, processorConfig, retrievalConfig);
        assertEquals(stageName, scheduler.applicationName());
    }

    @Test
    public final void testCreateOrGetShardConsumer() {
        final String shardId = "shardId-000000000000";
        final String concurrencyToken = "concurrencyToken";
        final ShardInfo shardInfo = new ShardInfo(shardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        final ShardConsumer shardConsumer1 = scheduler.createOrGetShardConsumer(shardInfo, shardRecordProcessorFactory);
        assertNotNull(shardConsumer1);
        final ShardConsumer shardConsumer2 = scheduler.createOrGetShardConsumer(shardInfo, shardRecordProcessorFactory);
        assertNotNull(shardConsumer2);

        assertSame(shardConsumer1, shardConsumer2);

        final String anotherConcurrencyToken = "anotherConcurrencyToken";
        final ShardInfo shardInfo2 = new ShardInfo(shardId, anotherConcurrencyToken, null,
                ExtendedSequenceNumber.TRIM_HORIZON);
        final ShardConsumer shardConsumer3 = scheduler.createOrGetShardConsumer(shardInfo2, shardRecordProcessorFactory);
        assertNotNull(shardConsumer3);

        assertNotSame(shardConsumer1, shardConsumer3);
    }

    // TODO: figure out the behavior of the test.
    @Test
    public void testWorkerLoopWithCheckpoint() throws Exception {
        final String shardId = "shardId-000000000000";
        final String concurrencyToken = "concurrencyToken";
        final ExtendedSequenceNumber firstSequenceNumber = ExtendedSequenceNumber.TRIM_HORIZON;
        final ExtendedSequenceNumber secondSequenceNumber = new ExtendedSequenceNumber("1000");
        final ExtendedSequenceNumber finalSequenceNumber = new ExtendedSequenceNumber("2000");

        final List<ShardInfo> initialShardInfo = Collections.singletonList(
                new ShardInfo(shardId, concurrencyToken, null, firstSequenceNumber));
        final List<ShardInfo> firstShardInfo = Collections.singletonList(
                new ShardInfo(shardId, concurrencyToken, null, secondSequenceNumber));
        final List<ShardInfo> secondShardInfo = Collections.singletonList(
                new ShardInfo(shardId, concurrencyToken, null, finalSequenceNumber));

        final Checkpoint firstCheckpoint = new Checkpoint(firstSequenceNumber, null);

        when(leaseCoordinator.getCurrentAssignments()).thenReturn(initialShardInfo, firstShardInfo, secondShardInfo);
        when(checkpoint.getCheckpointObject(eq(shardId))).thenReturn(firstCheckpoint);

        Scheduler schedulerSpy = spy(scheduler);
        schedulerSpy.runProcessLoop();
        schedulerSpy.runProcessLoop();
        schedulerSpy.runProcessLoop();

        verify(schedulerSpy).buildConsumer(same(initialShardInfo.get(0)), eq(shardRecordProcessorFactory));
        verify(schedulerSpy, never()).buildConsumer(same(firstShardInfo.get(0)), eq(shardRecordProcessorFactory));
        verify(schedulerSpy, never()).buildConsumer(same(secondShardInfo.get(0)), eq(shardRecordProcessorFactory));
        verify(checkpoint).getCheckpointObject(eq(shardId));
    }

    @Test
    public final void testCleanupShardConsumers() {
        final String shard0 = "shardId-000000000000";
        final String shard1 = "shardId-000000000001";
        final String concurrencyToken = "concurrencyToken";
        final String anotherConcurrencyToken = "anotherConcurrencyToken";

        final ShardInfo shardInfo0 = new ShardInfo(shard0, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        final ShardInfo shardInfo0WithAnotherConcurrencyToken = new ShardInfo(shard0, anotherConcurrencyToken, null,
                ExtendedSequenceNumber.TRIM_HORIZON);
        final ShardInfo shardInfo1 = new ShardInfo(shard1, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);

        final ShardConsumer shardConsumer0 = scheduler.createOrGetShardConsumer(shardInfo0, shardRecordProcessorFactory);
        final ShardConsumer shardConsumer0WithAnotherConcurrencyToken =
                scheduler.createOrGetShardConsumer(shardInfo0WithAnotherConcurrencyToken, shardRecordProcessorFactory);
        final ShardConsumer shardConsumer1 = scheduler.createOrGetShardConsumer(shardInfo1, shardRecordProcessorFactory);

        Set<ShardInfo> shards = new HashSet<>();
        shards.add(shardInfo0);
        shards.add(shardInfo1);
        scheduler.cleanupShardConsumers(shards);

        // verify shard consumer not present in assignedShards is shut down
        assertTrue(shardConsumer0WithAnotherConcurrencyToken.isShutdownRequested());
        // verify shard consumers present in assignedShards aren't shut down
        assertFalse(shardConsumer0.isShutdownRequested());
        assertFalse(shardConsumer1.isShutdownRequested());
    }

    @Test
    public final void testInitializationFailureWithRetries() throws Exception {
        doNothing().when(leaseCoordinator).initialize();
        when(shardDetector.listShards()).thenThrow(new RuntimeException());

        scheduler.run();

        verify(shardDetector, times(coordinatorConfig.maxInitializationAttempts())).listShards();
    }

    @Test
    public final void testInitializationFailureWithRetriesWithConfiguredMaxInitializationAttempts() throws Exception {
        final int maxInitializationAttempts = 5;
        coordinatorConfig.maxInitializationAttempts(maxInitializationAttempts);
        scheduler = new Scheduler(checkpointConfig, coordinatorConfig, leaseManagementConfig, lifecycleConfig,
                metricsConfig, processorConfig, retrievalConfig);

        doNothing().when(leaseCoordinator).initialize();
        when(shardDetector.listShards()).thenThrow(new RuntimeException());

        scheduler.run();

        // verify initialization was retried for maxInitializationAttempts times
        verify(shardDetector, times(maxInitializationAttempts)).listShards();
    }


    /*private void runAndTestWorker(int numShards, int threadPoolSize) throws Exception {
        final int numberOfRecordsPerShard = 10;
        final String kinesisShardPrefix = "kinesis-0-";
        final BigInteger startSeqNum = BigInteger.ONE;
        List<Shard> shardList = KinesisLocalFileDataCreator.createShardList(numShards, kinesisShardPrefix, startSeqNum);
        Assert.assertEquals(numShards, shardList.size());
        List<Lease> initialLeases = new ArrayList<Lease>();
        for (Shard shard : shardList) {
            Lease lease = ShardSyncer.newKCLLease(shard);
            lease.setCheckpoint(ExtendedSequenceNumber.AT_TIMESTAMP);
            initialLeases.add(lease);
        }
        runAndTestWorker(shardList, threadPoolSize, initialLeases, numberOfRecordsPerShard);
    }

    private void runAndTestWorker(List<Shard> shardList,
                                  int threadPoolSize,
                                  List<Lease> initialLeases,
                                  int numberOfRecordsPerShard) throws Exception {
        File file = KinesisLocalFileDataCreator.generateTempDataFile(shardList, numberOfRecordsPerShard, "unitTestWT001");
        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        Semaphore recordCounter = new Semaphore(0);
        ShardSequenceVerifier shardSequenceVerifier = new ShardSequenceVerifier(shardList);
        TestStreamletFactory recordProcessorFactory = new TestStreamletFactory(recordCounter, shardSequenceVerifier);

        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        SchedulerThread schedulerThread = runWorker(initialLeases);

        // TestStreamlet will release the semaphore once for every record it processes
        recordCounter.acquire(numberOfRecordsPerShard * shardList.size());

        // Wait a bit to allow the worker to spin against the end of the stream.
        Thread.sleep(500L);

        testWorker(shardList, threadPoolSize, initialLeases,
                numberOfRecordsPerShard, fileBasedProxy, recordProcessorFactory);

        schedulerThread.schedulerForThread().shutdown();
        executorService.shutdownNow();
        file.delete();
    }

    private SchedulerThread runWorker(final List<Lease> initialLeases) throws Exception {
        final int maxRecords = 2;

        final long leaseDurationMillis = 10000L;
        final long epsilonMillis = 1000L;
        final long idleTimeInMilliseconds = 2L;

        AmazonDynamoDB ddbClient = DynamoDBEmbedded.create().dynamoDBClient();
        LeaseManager<Lease> leaseRefresher = new LeaseManager("foo", ddbClient);
        leaseRefresher.createLeaseTableIfNotExists(1L, 1L);
        for (Lease initialLease : initialLeases) {
            leaseRefresher.createLeaseIfNotExists(initialLease);
        }

        checkpointConfig = new CheckpointConfig("foo", ddbClient, workerIdentifier)
                .failoverTimeMillis(leaseDurationMillis)
                .epsilonMillis(epsilonMillis)
                .leaseRefresher(leaseRefresher);
        leaseManagementConfig = new LeaseManagementConfig("foo", ddbClient, kinesisClient, streamName, workerIdentifier)
                .failoverTimeMillis(leaseDurationMillis)
                .epsilonMillis(epsilonMillis);
        retrievalConfig.initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPositionAtTimestamp(
                        new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP)))
                .maxRecords(maxRecords)
                .idleTimeBetweenReadsInMillis(idleTimeInMilliseconds);
        scheduler = new Scheduler(checkpointConfig, coordinatorConfig, leaseManagementConfig, lifecycleConfig,
                metricsConfig, processorConfig, retrievalConfig);

        SchedulerThread schedulerThread = new SchedulerThread(scheduler);
        schedulerThread.start();
        return schedulerThread;
    }

    private void testWorker(List<Shard> shardList,
                            int threadPoolSize,
                            List<Lease> initialLeases,
                            int numberOfRecordsPerShard,
                            IKinesisProxy kinesisProxy,
                            TestStreamletFactory recordProcessorFactory) throws Exception {
        recordProcessorFactory.getShardSequenceVerifier().verify();

        // Gather values to compare across all processors of a given shard.
        Map<String, List<Record>> shardStreamletsRecords = new HashMap<String, List<Record>>();
        Map<String, ShutdownReason> shardsLastProcessorShutdownReason = new HashMap<String, ShutdownReason>();
        Map<String, Long> shardsNumProcessRecordsCallsWithEmptyRecordList = new HashMap<String, Long>();
        for (TestStreamlet processor : recordProcessorFactory.getTestStreamlets()) {
            String shardId = processor.shardId();
            if (shardStreamletsRecords.get(shardId) == null) {
                shardStreamletsRecords.put(shardId, processor.getProcessedRecords());
            } else {
                List<Record> records = shardStreamletsRecords.get(shardId);
                records.addAll(processor.getProcessedRecords());
                shardStreamletsRecords.put(shardId, records);
            }
            if (shardsNumProcessRecordsCallsWithEmptyRecordList.get(shardId) == null) {
                shardsNumProcessRecordsCallsWithEmptyRecordList.put(shardId,
                        processor.getNumProcessRecordsCallsWithEmptyRecordList());
            } else {
                long totalShardsNumProcessRecordsCallsWithEmptyRecordList =
                        shardsNumProcessRecordsCallsWithEmptyRecordList.get(shardId)
                                + processor.getNumProcessRecordsCallsWithEmptyRecordList();
                shardsNumProcessRecordsCallsWithEmptyRecordList.put(shardId,
                        totalShardsNumProcessRecordsCallsWithEmptyRecordList);
            }
            shardsLastProcessorShutdownReason.put(processor.shardId(), processor.getShutdownReason());
        }

        // verify that all records were processed at least once
        verifyAllRecordsOfEachShardWereConsumedAtLeastOnce(shardList, kinesisProxy, numberOfRecordsPerShard, shardStreamletsRecords);
        shardList.forEach(shard -> {
            final String iterator = kinesisProxy.getIterator(shard.shardId(), new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP));
            final List<Record> records = kinesisProxy.get(iterator, numberOfRecordsPerShard).records();
            assertEquals();
        });
        for (Shard shard : shardList) {
            String shardId = shard.shardId();
            String iterator =
                    fileBasedProxy.getIterator(shardId, new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP));
            List<Record> expectedRecords = fileBasedProxy.get(iterator, numRecs).records();
            verifyAllRecordsWereConsumedAtLeastOnce(expectedRecords, shardStreamletsRecords.get(shardId));
        }

        // within a record processor all the incoming records should be ordered
        verifyRecordsProcessedByEachProcessorWereOrdered(recordProcessorFactory);

        // for shards for which only one record processor was created, we verify that each record should be
        // processed exactly once
        verifyAllRecordsOfEachShardWithOnlyOneProcessorWereConsumedExactlyOnce(shardList,
                kinesisProxy,
                numberOfRecordsPerShard,
                shardStreamletsRecords,
                recordProcessorFactory);

        // if callProcessRecordsForEmptyRecordList flag is set then processors must have been invoked with empty record
        // sets else they shouldn't have seen invoked with empty record sets
        verifyNumProcessRecordsCallsWithEmptyRecordList(shardList,
                shardsNumProcessRecordsCallsWithEmptyRecordList,
                callProcessRecordsForEmptyRecordList);

        // verify that worker shutdown last processor of shards that were terminated
        verifyLastProcessorOfClosedShardsWasShutdownWithTerminate(shardList, shardsLastProcessorShutdownReason);
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Accessors(fluent = true)
    private static class SchedulerThread extends Thread {
        private final Scheduler schedulerForThread;
    }*/

    private static class TestShardRecordProcessorFactory implements ShardRecordProcessorFactory {
        @Override
        public ShardRecordProcessor shardRecordProcessor() {
            return new ShardRecordProcessor() {
                @Override
                public void initialize(final InitializationInput initializationInput) {
                    // Do nothing.
                }

                @Override
                public void processRecords(final ProcessRecordsInput processRecordsInput) {
                    try {
                        processRecordsInput.checkpointer().checkpoint();
                    } catch (KinesisClientLibNonRetryableException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void leaseLost(LeaseLostInput leaseLostInput) {

                }

                @Override
                public void shardEnded(ShardEndedInput shardEndedInput) {
                    try {
                        shardEndedInput.checkpointer().checkpoint();
                    } catch (KinesisClientLibNonRetryableException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {

                }
            };
        }
    }

    private class TestKinesisLeaseManagementFactory implements LeaseManagementFactory {
        @Override
        public LeaseCoordinator createLeaseCoordinator(MetricsFactory metricsFactory) {
            return leaseCoordinator;
        }

        @Override
        public ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory) {
            return shardSyncTaskManager;
        }

        @Override
        public DynamoDBLeaseRefresher createLeaseRefresher() {
            return dynamoDBLeaseRefresher;
        }

        @Override
        public ShardDetector createShardDetector() {
            return shardDetector;
        }
    }

    private class TestKinesisCheckpointFactory implements CheckpointFactory {
        @Override
        public Checkpointer createCheckpointer(final LeaseCoordinator leaseCoordinator,
                                               final LeaseRefresher leaseRefresher) {
            return checkpoint;
        }
    }

}
