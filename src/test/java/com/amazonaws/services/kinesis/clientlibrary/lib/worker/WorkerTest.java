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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.Thread.State;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hamcrest.Condition;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibNonRetryableException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker.WorkerCWMetricsFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker.WorkerThreadPoolExecutor;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisLocalFileProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.util.KinesisLocalFileDataCreator;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseBuilder;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.leases.impl.LeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.RequiredArgsConstructor;

/**
 * Unit tests of Worker.
 */
@RunWith(MockitoJUnitRunner.class)
public class WorkerTest {

    private static final Log LOG = LogFactory.getLog(WorkerTest.class);

    // @Rule
    // public Timeout timeout = new Timeout((int)TimeUnit.SECONDS.toMillis(30));

    private final NullMetricsFactory nullMetricsFactory = new NullMetricsFactory();
    private final long taskBackoffTimeMillis = 1L;
    private final long failoverTimeMillis = 5L;
    private final boolean callProcessRecordsForEmptyRecordList = false;
    private final long parentShardPollIntervalMillis = 5L;
    private final long shardSyncIntervalMillis = 5L;
    private final boolean cleanupLeasesUponShardCompletion = true;
    // We don't want any of these tests to run checkpoint validation
    private final boolean skipCheckpointValidationValue = false;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private final ShardPrioritization shardPrioritization = new NoOpShardPrioritization();

    private static final String KINESIS_SHARD_ID_FORMAT = "kinesis-0-0-%d";
    private static final String CONCURRENCY_TOKEN_FORMAT = "testToken-%d";
    
    private RecordsFetcherFactory recordsFetcherFactory;
    private KinesisClientLibConfiguration config;

    @Mock
    private KinesisClientLibLeaseCoordinator leaseCoordinator;
    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;
    @Mock
    private com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory v1RecordProcessorFactory;
    @Mock
    private IKinesisProxy proxy;
    @Mock
    private WorkerThreadPoolExecutor executorService;
    @Mock
    private WorkerCWMetricsFactory cwMetricsFactory;
    @Mock
    private IKinesisProxy kinesisProxy;
    @Mock
    private IRecordProcessorFactory v2RecordProcessorFactory;
    @Mock
    private IRecordProcessor v2RecordProcessor;
    @Mock
    private ShardConsumer shardConsumer;
    @Mock
    private Future<TaskResult> taskFuture;
    @Mock
    private TaskResult taskResult;
    
    @Before
    public void setup() {
        config = spy(new KinesisClientLibConfiguration("app", null, null, null));
        recordsFetcherFactory = spy(new SimpleRecordsFetcherFactory(500));
        when(config.getRecordsFetcherFactory()).thenReturn(recordsFetcherFactory);
    }

    // CHECKSTYLE:IGNORE AnonInnerLengthCheck FOR NEXT 50 LINES
    private static final com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory SAMPLE_RECORD_PROCESSOR_FACTORY = 
            new com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory() {

        @Override
        public com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor createProcessor() {
            return new com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor() {

                @Override
                public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
                    if (reason == ShutdownReason.TERMINATE) {
                        try {
                            checkpointer.checkpoint();
                        } catch (KinesisClientLibNonRetryableException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public void processRecords(List<Record> dataRecords, IRecordProcessorCheckpointer checkpointer) {
                    try {
                        checkpointer.checkpoint();
                    } catch (KinesisClientLibNonRetryableException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void initialize(String shardId) {
                }
            };
        }
    };
    
    private static final IRecordProcessorFactory SAMPLE_RECORD_PROCESSOR_FACTORY_V2 = 
            new V1ToV2RecordProcessorFactoryAdapter(SAMPLE_RECORD_PROCESSOR_FACTORY);


    /**
     * Test method for {@link Worker#getApplicationName()}.
     */
    @Test
    public final void testGetStageName() {
        final String stageName = "testStageName";
        config = new KinesisClientLibConfiguration(stageName, null, null, null);
        Worker worker = new Worker(v1RecordProcessorFactory, config);
        Assert.assertEquals(stageName, worker.getApplicationName());
    }

    @Test
    public final void testCreateOrGetShardConsumer() {
        final String stageName = "testStageName";
        IRecordProcessorFactory streamletFactory = SAMPLE_RECORD_PROCESSOR_FACTORY_V2;
        config = new KinesisClientLibConfiguration(stageName, null, null, null);
        IKinesisProxy proxy = null;
        ICheckpoint checkpoint = null;
        int maxRecords = 1;
        int idleTimeInMilliseconds = 1000;
        StreamConfig streamConfig =
                new StreamConfig(proxy,
                        maxRecords,
                        idleTimeInMilliseconds,
                        callProcessRecordsForEmptyRecordList, skipCheckpointValidationValue, INITIAL_POSITION_LATEST);
        final String testConcurrencyToken = "testToken";
        final String anotherConcurrencyToken = "anotherTestToken";
        final String dummyKinesisShardId = "kinesis-0-0";
        ExecutorService execService = null;

        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);

        Worker worker =
                new Worker(stageName,
                        streamletFactory,
                        config,
                        streamConfig, INITIAL_POSITION_LATEST,
                        parentShardPollIntervalMillis,
                        shardSyncIntervalMillis,
                        cleanupLeasesUponShardCompletion,
                        checkpoint,
                        leaseCoordinator,
                        execService,
                        nullMetricsFactory,
                        taskBackoffTimeMillis,
                        failoverTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        shardPrioritization);
        ShardInfo shardInfo = new ShardInfo(dummyKinesisShardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardConsumer consumer = worker.createOrGetShardConsumer(shardInfo, streamletFactory);
        Assert.assertNotNull(consumer);
        ShardConsumer consumer2 = worker.createOrGetShardConsumer(shardInfo, streamletFactory);
        Assert.assertSame(consumer, consumer2);
        ShardInfo shardInfoWithSameShardIdButDifferentConcurrencyToken =
                new ShardInfo(dummyKinesisShardId, anotherConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardConsumer consumer3 =
                worker.createOrGetShardConsumer(shardInfoWithSameShardIdButDifferentConcurrencyToken, streamletFactory);
        Assert.assertNotNull(consumer3);
        Assert.assertNotSame(consumer3, consumer);
    }

    @Test
    public void testWorkerLoopWithCheckpoint() {
        final String stageName = "testStageName";
        IRecordProcessorFactory streamletFactory = SAMPLE_RECORD_PROCESSOR_FACTORY_V2;
        IKinesisProxy proxy = null;
        ICheckpoint checkpoint = null;
        int maxRecords = 1;
        int idleTimeInMilliseconds = 1000;
        StreamConfig streamConfig = new StreamConfig(proxy, maxRecords, idleTimeInMilliseconds,
                callProcessRecordsForEmptyRecordList, skipCheckpointValidationValue, INITIAL_POSITION_LATEST);

        ExecutorService execService = null;

        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);

        List<ShardInfo> initialState = createShardInfoList(ExtendedSequenceNumber.TRIM_HORIZON);
        List<ShardInfo> firstCheckpoint = createShardInfoList(new ExtendedSequenceNumber("1000"));
        List<ShardInfo> secondCheckpoint = createShardInfoList(new ExtendedSequenceNumber("2000"));

        when(leaseCoordinator.getCurrentAssignments()).thenReturn(initialState).thenReturn(firstCheckpoint)
                .thenReturn(secondCheckpoint);

        Worker worker = new Worker(stageName,
                streamletFactory,
                config,
                streamConfig,
                INITIAL_POSITION_LATEST,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                checkpoint,
                leaseCoordinator,
                execService,
                nullMetricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                shardPrioritization);

        Worker workerSpy = spy(worker);

        doReturn(shardConsumer).when(workerSpy).buildConsumer(eq(initialState.get(0)), any(IRecordProcessorFactory.class));
        workerSpy.runProcessLoop();
        workerSpy.runProcessLoop();
        workerSpy.runProcessLoop();

        verify(workerSpy).buildConsumer(same(initialState.get(0)), any(IRecordProcessorFactory.class));
        verify(workerSpy, never()).buildConsumer(same(firstCheckpoint.get(0)), any(IRecordProcessorFactory.class));
        verify(workerSpy, never()).buildConsumer(same(secondCheckpoint.get(0)), any(IRecordProcessorFactory.class));

    }

    private List<ShardInfo> createShardInfoList(ExtendedSequenceNumber... sequenceNumbers) {
        List<ShardInfo> result = new ArrayList<>(sequenceNumbers.length);
        assertThat(sequenceNumbers.length, greaterThanOrEqualTo(1));
        for (int i = 0; i < sequenceNumbers.length; ++i) {
            result.add(new ShardInfo(adjustedShardId(i), adjustedConcurrencyToken(i), null, sequenceNumbers[i]));
        }
        return result;
    }

    private String adjustedShardId(int index) {
        return String.format(KINESIS_SHARD_ID_FORMAT, index);
    }

    private String adjustedConcurrencyToken(int index) {
        return String.format(CONCURRENCY_TOKEN_FORMAT, index);
    }

    @Test
    public final void testCleanupShardConsumers() {
        final String stageName = "testStageName";
        IRecordProcessorFactory streamletFactory = SAMPLE_RECORD_PROCESSOR_FACTORY_V2;
        config = new KinesisClientLibConfiguration(stageName, null, null, null);
        IKinesisProxy proxy = null;
        ICheckpoint checkpoint = null;
        int maxRecords = 1;
        int idleTimeInMilliseconds = 1000;
        StreamConfig streamConfig =
                new StreamConfig(proxy,
                        maxRecords,
                        idleTimeInMilliseconds,
                        callProcessRecordsForEmptyRecordList, skipCheckpointValidationValue, INITIAL_POSITION_LATEST);
        final String concurrencyToken = "testToken";
        final String anotherConcurrencyToken = "anotherTestToken";
        final String dummyKinesisShardId = "kinesis-0-0";
        final String anotherDummyKinesisShardId = "kinesis-0-1";
        ExecutorService execService = null;
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);

        Worker worker =
                new Worker(stageName,
                        streamletFactory,
                        config,
                        streamConfig, INITIAL_POSITION_LATEST,
                        parentShardPollIntervalMillis,
                        shardSyncIntervalMillis,
                        cleanupLeasesUponShardCompletion,
                        checkpoint,
                        leaseCoordinator,
                        execService,
                        nullMetricsFactory,
                        taskBackoffTimeMillis,
                        failoverTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        shardPrioritization);

        ShardInfo shardInfo1 = new ShardInfo(dummyKinesisShardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardInfo duplicateOfShardInfo1ButWithAnotherConcurrencyToken =
                new ShardInfo(dummyKinesisShardId, anotherConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardInfo shardInfo2 = new ShardInfo(anotherDummyKinesisShardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);

        ShardConsumer consumerOfShardInfo1 = worker.createOrGetShardConsumer(shardInfo1, streamletFactory);
        ShardConsumer consumerOfDuplicateOfShardInfo1ButWithAnotherConcurrencyToken =
                worker.createOrGetShardConsumer(duplicateOfShardInfo1ButWithAnotherConcurrencyToken, streamletFactory);
        ShardConsumer consumerOfShardInfo2 = worker.createOrGetShardConsumer(shardInfo2, streamletFactory);

        Set<ShardInfo> assignedShards = new HashSet<ShardInfo>();
        assignedShards.add(shardInfo1);
        assignedShards.add(shardInfo2);
        worker.cleanupShardConsumers(assignedShards);

        // verify shard consumer not present in assignedShards is shut down
        Assert.assertTrue(consumerOfDuplicateOfShardInfo1ButWithAnotherConcurrencyToken.isShutdownRequested());
        // verify shard consumers present in assignedShards aren't shut down
        Assert.assertFalse(consumerOfShardInfo1.isShutdownRequested());
        Assert.assertFalse(consumerOfShardInfo2.isShutdownRequested());
    }

    @Test
    public final void testInitializationFailureWithRetries() {
        String stageName = "testInitializationWorker";
        IRecordProcessorFactory recordProcessorFactory = new TestStreamletFactory(null, null);
        config = new KinesisClientLibConfiguration(stageName, null, null, null);
        int count = 0;
        when(proxy.getShardList()).thenThrow(new RuntimeException(Integer.toString(count++)));
        int maxRecords = 2;
        long idleTimeInMilliseconds = 1L;
        StreamConfig streamConfig =
                new StreamConfig(proxy,
                        maxRecords,
                        idleTimeInMilliseconds,
                        callProcessRecordsForEmptyRecordList, skipCheckpointValidationValue, INITIAL_POSITION_LATEST);
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        ExecutorService execService = Executors.newSingleThreadExecutor();
        long shardPollInterval = 0L;
        Worker worker =
                new Worker(stageName,
                        recordProcessorFactory,
                        config,
                        streamConfig, INITIAL_POSITION_TRIM_HORIZON,
                        shardPollInterval,
                        shardSyncIntervalMillis,
                        cleanupLeasesUponShardCompletion,
                        leaseCoordinator,
                        leaseCoordinator,
                        execService,
                        nullMetricsFactory,
                        taskBackoffTimeMillis,
                        failoverTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        shardPrioritization);
        worker.run();
        Assert.assertTrue(count > 0);
    }

    /**
     * Runs worker with threadPoolSize == numShards
     * Test method for {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker#run()}.
     */
    @Test
    public final void testRunWithThreadPoolSizeEqualToNumShards() throws Exception {
        final int numShards = 1;
        final int threadPoolSize = numShards;
        runAndTestWorker(numShards, threadPoolSize);
    }

    /**
     * Runs worker with threadPoolSize < numShards
     * Test method for {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker#run()}.
     */
    @Test
    public final void testRunWithThreadPoolSizeLessThanNumShards() throws Exception {
        final int numShards = 3;
        final int threadPoolSize = 2;
        runAndTestWorker(numShards, threadPoolSize);
    }

    /**
     * Runs worker with threadPoolSize > numShards
     * Test method for {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker#run()}.
     */
    @Test
    public final void testRunWithThreadPoolSizeMoreThanNumShards() throws Exception {
        final int numShards = 3;
        final int threadPoolSize = 5;
        runAndTestWorker(numShards, threadPoolSize);
    }

    /**
     * Runs worker with threadPoolSize < numShards
     * Test method for {@link Worker#run()}.
     */
    @Test
    public final void testOneSplitShard2Threads() throws Exception {
        final int threadPoolSize = 2;
        final int numberOfRecordsPerShard = 10;
        List<Shard> shardList = createShardListWithOneSplit();
        List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        KinesisClientLease lease = ShardSyncer.newKCLLease(shardList.get(0));
        lease.setCheckpoint(new ExtendedSequenceNumber("2"));
        initialLeases.add(lease);
        runAndTestWorker(shardList, threadPoolSize, initialLeases, callProcessRecordsForEmptyRecordList, numberOfRecordsPerShard, config);
    }

    /**
     * Runs worker with threadPoolSize < numShards
     * Test method for {@link Worker#run()}.
     */
    @Test
    public final void testOneSplitShard2ThreadsWithCallsForEmptyRecords() throws Exception {
        final int threadPoolSize = 2;
        final int numberOfRecordsPerShard = 10;
        List<Shard> shardList = createShardListWithOneSplit();
        List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        KinesisClientLease lease = ShardSyncer.newKCLLease(shardList.get(0));
        lease.setCheckpoint(new ExtendedSequenceNumber("2"));
        initialLeases.add(lease);
        boolean callProcessRecordsForEmptyRecordList = true;
        RecordsFetcherFactory recordsFetcherFactory = new SimpleRecordsFetcherFactory(500);
        recordsFetcherFactory.setIdleMillisBetweenCalls(0L);
        when(config.getRecordsFetcherFactory()).thenReturn(recordsFetcherFactory);
        runAndTestWorker(shardList, threadPoolSize, initialLeases, callProcessRecordsForEmptyRecordList, numberOfRecordsPerShard, config);
    }

    @Test
    public final void testWorkerShutsDownOwnedResources() throws Exception {

        final long failoverTimeMillis = 20L;

        // Make sure that worker thread is run before invoking shutdown.
        final CountDownLatch workerStarted = new CountDownLatch(1);
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                workerStarted.countDown();
                return false;
            }
        }).when(executorService).isShutdown();

        final WorkerThread workerThread = runWorker(Collections.<Shard>emptyList(),
                Collections.<KinesisClientLease>emptyList(),
                callProcessRecordsForEmptyRecordList,
                failoverTimeMillis,
                10,
                kinesisProxy, v2RecordProcessorFactory,
                executorService,
                cwMetricsFactory,
                config);

        // Give some time for thread to run.
        workerStarted.await();

        workerThread.getWorker().shutdown();
        workerThread.join();

        Assert.assertTrue(workerThread.getState() == State.TERMINATED);
        verify(executorService, times(1)).shutdownNow();
        verify(cwMetricsFactory, times(1)).shutdown();
    }

    @Test
    public final void testWorkerDoesNotShutdownClientResources() throws Exception {
        final long failoverTimeMillis = 20L;

        final ExecutorService executorService = mock(ThreadPoolExecutor.class);
        final CWMetricsFactory cwMetricsFactory = mock(CWMetricsFactory.class);
        // Make sure that worker thread is run before invoking shutdown.
        final CountDownLatch workerStarted = new CountDownLatch(1);
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                workerStarted.countDown();
                return false;
            }
        }).when(executorService).isShutdown();

        final WorkerThread workerThread = runWorker(Collections.<Shard>emptyList(),
                Collections.<KinesisClientLease>emptyList(),
                callProcessRecordsForEmptyRecordList,
                failoverTimeMillis,
                10,
                kinesisProxy, v2RecordProcessorFactory,
                executorService,
                cwMetricsFactory,
                config);

        // Give some time for thread to run.
        workerStarted.await();

        workerThread.getWorker().shutdown();
        workerThread.join();

        Assert.assertTrue(workerThread.getState() == State.TERMINATED);
        verify(executorService, times(0)).shutdownNow();
        verify(cwMetricsFactory, times(0)).shutdown();
    }

    @Test
    public final void testWorkerNormalShutdown() throws Exception {
        final List<Shard> shardList = createShardListWithOneShard();
        final boolean callProcessRecordsForEmptyRecordList = true;
        final long failoverTimeMillis = 50L;
        final int numberOfRecordsPerShard = 1000;

        final List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        for (Shard shard : shardList) {
            KinesisClientLease lease = ShardSyncer.newKCLLease(shard);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            initialLeases.add(lease);
        }

        final File file = KinesisLocalFileDataCreator.generateTempDataFile(
                shardList, numberOfRecordsPerShard, "normalShutdownUnitTest");
        final IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        final ExecutorService executorService = Executors.newCachedThreadPool();

        // Make test case as efficient as possible.
        final CountDownLatch processRecordsLatch = new CountDownLatch(1);

        when(v2RecordProcessorFactory.createProcessor()).thenReturn(v2RecordProcessor);

        doAnswer(new Answer<Object> () {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                // Signal that record processor has started processing records.
                processRecordsLatch.countDown();
                return null;
            }
        }).when(v2RecordProcessor).processRecords(any(ProcessRecordsInput.class));
        
        RecordsFetcherFactory recordsFetcherFactory = mock(RecordsFetcherFactory.class);
        GetRecordsCache getRecordsCache = mock(GetRecordsCache.class);
        when(config.getRecordsFetcherFactory()).thenReturn(recordsFetcherFactory);
        when(recordsFetcherFactory.createRecordsFetcher(any(), anyString(),any())).thenReturn(getRecordsCache);
        when(getRecordsCache.getNextResult()).thenReturn(new ProcessRecordsInput().withRecords(Collections.emptyList()).withMillisBehindLatest(0L));

        WorkerThread workerThread = runWorker(shardList,
                initialLeases,
                callProcessRecordsForEmptyRecordList,
                failoverTimeMillis,
                numberOfRecordsPerShard,
                fileBasedProxy,
                v2RecordProcessorFactory,
                executorService,
                nullMetricsFactory,
                config);

        // Only sleep for time that is required.
        processRecordsLatch.await();

        // Make sure record processor is initialized and processing records.
        verify(v2RecordProcessorFactory, times(1)).createProcessor();
        verify(v2RecordProcessor, times(1)).initialize(any(InitializationInput.class));
        verify(v2RecordProcessor, atLeast(1)).processRecords(any(ProcessRecordsInput.class));
        verify(v2RecordProcessor, times(0)).shutdown(any(ShutdownInput.class));

        workerThread.getWorker().shutdown();
        workerThread.join();

        Assert.assertTrue(workerThread.getState() == State.TERMINATED);
        verify(v2RecordProcessor, times(1)).shutdown(any(ShutdownInput.class));
    }

    /**
     * This test is testing the {@link Worker}'s shutdown behavior and by extension the behavior of
     * {@link ThreadPoolExecutor#shutdownNow()}. It depends on the thread pool sending an interrupt to the pool threads.
     * This behavior makes the test a bit racy, since we need to ensure a specific order of events.
     * 
     * @throws Exception
     */
    @Test
    public final void testWorkerForcefulShutdown() throws Exception {
        final List<Shard> shardList = createShardListWithOneShard();
        final boolean callProcessRecordsForEmptyRecordList = true;
        final long failoverTimeMillis = 50L;
        final int numberOfRecordsPerShard = 10;

        final List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        for (Shard shard : shardList) {
            KinesisClientLease lease = ShardSyncer.newKCLLease(shard);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            initialLeases.add(lease);
        }

        final File file = KinesisLocalFileDataCreator.generateTempDataFile(
                shardList, numberOfRecordsPerShard, "normalShutdownUnitTest");
        final IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        // Get executor service that will be owned by the worker, so we can get interrupts.
        ExecutorService executorService = getWorkerThreadPoolExecutor();

        // Make test case as efficient as possible.
        final CountDownLatch processRecordsLatch = new CountDownLatch(1);
        final AtomicBoolean recordProcessorInterrupted = new AtomicBoolean(false);
        when(v2RecordProcessorFactory.createProcessor()).thenReturn(v2RecordProcessor);
        final Semaphore actionBlocker = new Semaphore(1);
        final Semaphore shutdownBlocker = new Semaphore(1);

        actionBlocker.acquire();

        doAnswer(new Answer<Object> () {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                // Signal that record processor has started processing records.
                processRecordsLatch.countDown();

                // Block for some time now to test forceful shutdown. Also, check if record processor
                // was interrupted or not.
                final long startTimeMillis = System.currentTimeMillis();
                long elapsedTimeMillis = 0;

                LOG.info("Entering sleep @ " + startTimeMillis + " with elapsedMills: " + elapsedTimeMillis);
                shutdownBlocker.acquire();
                try {
                    actionBlocker.acquire();
                } catch (InterruptedException e) {
                    LOG.info("Sleep interrupted @ " + System.currentTimeMillis() + " elapsedMillis: "
                            + (System.currentTimeMillis() - startTimeMillis));
                    recordProcessorInterrupted.getAndSet(true);
                }
                shutdownBlocker.release();
                elapsedTimeMillis = System.currentTimeMillis() - startTimeMillis;
                LOG.info("Sleep completed @ " + System.currentTimeMillis() + " elapsedMillis: " + elapsedTimeMillis);

                return null;
            }
        }).when(v2RecordProcessor).processRecords(any(ProcessRecordsInput.class));

        WorkerThread workerThread = runWorker(shardList,
                initialLeases,
                callProcessRecordsForEmptyRecordList,
                failoverTimeMillis,
                numberOfRecordsPerShard,
                fileBasedProxy,
                v2RecordProcessorFactory,
                executorService,
                nullMetricsFactory,
                config);

        // Only sleep for time that is required.
        processRecordsLatch.await();

        // Make sure record processor is initialized and processing records.
        verify(v2RecordProcessorFactory, times(1)).createProcessor();
        verify(v2RecordProcessor, times(1)).initialize(any(InitializationInput.class));
        verify(v2RecordProcessor, atLeast(1)).processRecords(any(ProcessRecordsInput.class));
        verify(v2RecordProcessor, times(0)).shutdown(any(ShutdownInput.class));

        workerThread.getWorker().shutdown();
        workerThread.join();

        Assert.assertTrue(workerThread.getState() == State.TERMINATED);
        // Shutdown should not be called in this case because record processor is blocked.
        verify(v2RecordProcessor, times(0)).shutdown(any(ShutdownInput.class));

        //
        // Release the worker thread
        //
        actionBlocker.release();
        //
        // Give the worker thread time to execute it's interrupted handler.
        //
        shutdownBlocker.tryAcquire(100, TimeUnit.MILLISECONDS);
        //
        // Now we can see if it was actually interrupted. It's possible it wasn't and this will fail.
        //
        assertThat(recordProcessorInterrupted.get(), equalTo(true));
    }

    @Test
    public void testRequestShutdown() throws Exception {


        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().withCheckpoint(checkpoint)
                .withConcurrencyToken(UUID.randomUUID()).withLastCounterIncrementNanos(0L).withLeaseCounter(0L)
                .withOwnerSwitchesSinceCheckpoint(0L).withLeaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.withLeaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.getLeaseKey(), lease.getConcurrencyToken().toString(),
                lease.getParentShardIds(), lease.getCheckpoint()));


        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);


        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        worker.createWorkerShutdownCallable().call();
        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION))));

        worker.runProcessLoop();

        verify(leaseCoordinator, atLeastOnce()).dropLease(eq(lease));
        leases.clear();
        currentAssignments.clear();

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

    }

    @Test(expected = IllegalStateException.class)
    public void testShutdownCallableNotAllowedTwice() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().withCheckpoint(checkpoint)
                .withConcurrencyToken(UUID.randomUUID()).withLastCounterIncrementNanos(0L).withLeaseCounter(0L)
                .withOwnerSwitchesSinceCheckpoint(0L).withLeaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.withLeaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.getLeaseKey(), lease.getConcurrencyToken().toString(),
                lease.getParentShardIds(), lease.getCheckpoint()));

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new InjectableWorker("testRequestShutdown", recordProcessorFactory, config, streamConfig,
                INITIAL_POSITION_TRIM_HORIZON, parentShardPollIntervalMillis, shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion, leaseCoordinator, leaseCoordinator, executorService, metricsFactory,
                taskBackoffTimeMillis, failoverTimeMillis, false, shardPrioritization) {
            @Override
            void postConstruct() {
                this.gracefuleShutdownStarted = true;
            }
        };

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        assertThat(worker.hasGracefulShutdownStarted(), equalTo(true));
        worker.createWorkerShutdownCallable().call();

    }

    @Test
    public void testGracefulShutdownSingleFuture() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().withCheckpoint(checkpoint)
                .withConcurrencyToken(UUID.randomUUID()).withLastCounterIncrementNanos(0L).withLeaseCounter(0L)
                .withOwnerSwitchesSinceCheckpoint(0L).withLeaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.withLeaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.getLeaseKey(), lease.getConcurrencyToken().toString(),
                lease.getParentShardIds(), lease.getCheckpoint()));

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        GracefulShutdownCoordinator coordinator = mock(GracefulShutdownCoordinator.class);
        when(coordinator.createGracefulShutdownCallable(any(Callable.class))).thenReturn(() -> true);

        Future<Boolean> gracefulShutdownFuture = mock(Future.class);

        when(coordinator.startGracefulShutdown(any(Callable.class))).thenReturn(gracefulShutdownFuture);

        Worker worker = new InjectableWorker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization) {
            @Override
            void postConstruct() {
                this.gracefulShutdownCoordinator = coordinator;
            }
        };

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        Future<Boolean> firstFuture = worker.startGracefulShutdown();
        Future<Boolean> secondFuture = worker.startGracefulShutdown();

        assertThat(firstFuture, equalTo(secondFuture));
        verify(coordinator).startGracefulShutdown(any(Callable.class));

    }

    @Test
    public void testRequestShutdownNoLeases() throws Exception {


        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);


        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);


        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        worker.createWorkerShutdownCallable().call();
        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION))));

        worker.runProcessLoop();
        verify(executorService, never()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

        assertThat(worker.shouldShutdown(), equalTo(true));

    }

    @Test
    public void testRequestShutdownWithLostLease() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLease lease1 = makeLease(checkpoint, 1);
        KinesisClientLease lease2 = makeLease(checkpoint, 2);
        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        leases.add(lease1);
        leases.add(lease2);

        ShardInfo shardInfo1 = makeShardInfo(lease1);
        currentAssignments.add(shardInfo1);
        ShardInfo shardInfo2 = makeShardInfo(lease2);
        currentAssignments.add(shardInfo2);

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS)).and(ReflectionFieldMatcher
                        .withField(BlockOnParentShardTask.class, "shardInfo", equalTo(shardInfo1)))));
        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS)).and(ReflectionFieldMatcher
                        .withField(BlockOnParentShardTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.runProcessLoop();

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE)).and(ReflectionFieldMatcher
                        .withField(InitializeTask.class, "shardInfo", equalTo(shardInfo1)))));
        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE)).and(ReflectionFieldMatcher
                        .withField(InitializeTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.getShardInfoShardConsumerMap().remove(shardInfo2);
        worker.createWorkerShutdownCallable().call();
        leases.remove(1);
        currentAssignments.remove(1);
        worker.runProcessLoop();


        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION)).and(ReflectionFieldMatcher
                        .withField(ShutdownNotificationTask.class, "shardInfo", equalTo(shardInfo1)))));

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION)).and(ReflectionFieldMatcher
                        .withField(ShutdownNotificationTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.runProcessLoop();

        verify(leaseCoordinator).dropLease(eq(lease1));
        verify(leaseCoordinator, never()).dropLease(eq(lease2));
        leases.clear();
        currentAssignments.clear();

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN)).and(ReflectionFieldMatcher
                        .withField(ShutdownTask.class, "shardInfo", equalTo(shardInfo1)))));

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN)).and(ReflectionFieldMatcher
                        .withField(ShutdownTask.class, "shardInfo", equalTo(shardInfo2)))));

    }

    @Test
    public void testRequestShutdownWithAllLeasesLost() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLease lease1 = makeLease(checkpoint, 1);
        KinesisClientLease lease2 = makeLease(checkpoint, 2);
        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        leases.add(lease1);
        leases.add(lease2);

        ShardInfo shardInfo1 = makeShardInfo(lease1);
        currentAssignments.add(shardInfo1);
        ShardInfo shardInfo2 = makeShardInfo(lease2);
        currentAssignments.add(shardInfo2);

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS)).and(ReflectionFieldMatcher
                        .withField(BlockOnParentShardTask.class, "shardInfo", equalTo(shardInfo1)))));
        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS)).and(ReflectionFieldMatcher
                        .withField(BlockOnParentShardTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.runProcessLoop();

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE)).and(ReflectionFieldMatcher
                        .withField(InitializeTask.class, "shardInfo", equalTo(shardInfo1)))));
        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE)).and(ReflectionFieldMatcher
                        .withField(InitializeTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.getShardInfoShardConsumerMap().clear();
        Future<Void> future = worker.requestShutdown();

        leases.clear();
        currentAssignments.clear();

        try {
            future.get(1, TimeUnit.HOURS);
        } catch (TimeoutException te) {
            fail("Future from requestShutdown should immediately return.");
        }

        worker.runProcessLoop();
        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION)).and(ReflectionFieldMatcher
                        .withField(ShutdownNotificationTask.class, "shardInfo", equalTo(shardInfo1)))));

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION)).and(ReflectionFieldMatcher
                        .withField(ShutdownNotificationTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.runProcessLoop();

        verify(leaseCoordinator, never()).dropLease(eq(lease1));
        verify(leaseCoordinator, never()).dropLease(eq(lease2));

        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN)).and(ReflectionFieldMatcher
                        .withField(ShutdownTask.class, "shardInfo", equalTo(shardInfo1)))));

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN)).and(ReflectionFieldMatcher
                        .withField(ShutdownTask.class, "shardInfo", equalTo(shardInfo2)))));



        assertThat(worker.shouldShutdown(), equalTo(true));

    }

    @Test
    public void testLeaseCancelledAfterShutdownRequest() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().withCheckpoint(checkpoint)
                .withConcurrencyToken(UUID.randomUUID()).withLastCounterIncrementNanos(0L).withLeaseCounter(0L)
                .withOwnerSwitchesSinceCheckpoint(0L).withLeaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.withLeaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.getLeaseKey(), lease.getConcurrencyToken().toString(),
                lease.getParentShardIds(), lease.getCheckpoint()));

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis, 
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        worker.requestShutdown();
        leases.clear();
        currentAssignments.clear();
        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce())
                .submit(argThat(ShutdownReasonMatcher.hasReason(equalTo(ShutdownReason.ZOMBIE))));
        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

    }

    @Test
    public void testEndOfShardAfterShutdownRequest() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().withCheckpoint(checkpoint)
                .withConcurrencyToken(UUID.randomUUID()).withLastCounterIncrementNanos(0L).withLeaseCounter(0L)
                .withOwnerSwitchesSinceCheckpoint(0L).withLeaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.withLeaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.getLeaseKey(), lease.getConcurrencyToken().toString(),
                lease.getParentShardIds(), lease.getCheckpoint()));

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis, 
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));
        when(taskResult.isShardEndReached()).thenReturn(true);

        worker.requestShutdown();
        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION))));

        verify(executorService).submit(argThat(ShutdownReasonMatcher.hasReason(equalTo(ShutdownReason.TERMINATE))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

    }

    private abstract class InjectableWorker extends Worker {
        InjectableWorker(String applicationName, IRecordProcessorFactory recordProcessorFactory,
                KinesisClientLibConfiguration config, StreamConfig streamConfig,
                InitialPositionInStreamExtended initialPositionInStream,
                long parentShardPollIntervalMillis, long shardSyncIdleTimeMillis,
                boolean cleanupLeasesUponShardCompletion, ICheckpoint checkpoint,
                KinesisClientLibLeaseCoordinator leaseCoordinator, ExecutorService execService,
                IMetricsFactory metricsFactory, long taskBackoffTimeMillis, long failoverTimeMillis,
                boolean skipShardSyncAtWorkerInitializationIfLeasesExist, ShardPrioritization shardPrioritization) {
            super(applicationName,
                    recordProcessorFactory,
                    config,
                    streamConfig,
                    initialPositionInStream,
                    parentShardPollIntervalMillis,
                    shardSyncIdleTimeMillis,
                    cleanupLeasesUponShardCompletion,
                    checkpoint,
                    leaseCoordinator,
                    execService,
                    metricsFactory,
                    taskBackoffTimeMillis,
                    failoverTimeMillis,
                    skipShardSyncAtWorkerInitializationIfLeasesExist,
                    shardPrioritization);
            postConstruct();
        }

        abstract void postConstruct();
    }

    private KinesisClientLease makeLease(ExtendedSequenceNumber checkpoint, int shardId) {
        return new KinesisClientLeaseBuilder().withCheckpoint(checkpoint).withConcurrencyToken(UUID.randomUUID())
                .withLastCounterIncrementNanos(0L).withLeaseCounter(0L).withOwnerSwitchesSinceCheckpoint(0L)
                .withLeaseOwner("Self").withLeaseKey(String.format("shardId-%03d", shardId)).build();
    }

    private ShardInfo makeShardInfo(KinesisClientLease lease) {
        return new ShardInfo(lease.getLeaseKey(), lease.getConcurrencyToken().toString(), lease.getParentShardIds(),
                lease.getCheckpoint());
    }

    private static class ShutdownReasonMatcher extends TypeSafeDiagnosingMatcher<MetricsCollectingTaskDecorator> {

        private final Matcher<ShutdownReason> matcher;

        public ShutdownReasonMatcher(Matcher<ShutdownReason> matcher) {
            this.matcher = matcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("hasShutdownReason(").appendDescriptionOf(matcher).appendText(")");
        }

        @Override
        protected boolean matchesSafely(MetricsCollectingTaskDecorator item, Description mismatchDescription) {
            return Condition.matched(item, mismatchDescription)
                    .and(new Condition.Step<MetricsCollectingTaskDecorator, ShutdownTask>() {
                        @Override
                        public Condition<ShutdownTask> apply(MetricsCollectingTaskDecorator value,
                                Description mismatch) {
                            if (!(value.getOther() instanceof ShutdownTask)) {
                                mismatch.appendText("Wrapped task isn't a shutdown task");
                                return Condition.notMatched();
                            }
                            return Condition.matched((ShutdownTask) value.getOther(), mismatch);
                        }
                    }).and(new Condition.Step<ShutdownTask, ShutdownReason>() {
                        @Override
                        public Condition<ShutdownReason> apply(ShutdownTask value, Description mismatch) {
                            return Condition.matched(value.getReason(), mismatch);
                        }
                    }).matching(matcher);
        }

        public static ShutdownReasonMatcher hasReason(Matcher<ShutdownReason> matcher) {
            return new ShutdownReasonMatcher(matcher);
        }
    }

    private static class ShutdownHandlingAnswer implements Answer<Future<TaskResult>> {

        final Future<TaskResult> defaultFuture;

        public ShutdownHandlingAnswer(Future<TaskResult> defaultFuture) {
            this.defaultFuture = defaultFuture;
        }

        @Override
        public Future<TaskResult> answer(InvocationOnMock invocation) throws Throwable {
            ITask rootTask = (ITask) invocation.getArguments()[0];
            if (rootTask instanceof MetricsCollectingTaskDecorator
                    && ((MetricsCollectingTaskDecorator) rootTask).getOther() instanceof ShutdownNotificationTask) {
                ShutdownNotificationTask task = (ShutdownNotificationTask) ((MetricsCollectingTaskDecorator) rootTask).getOther();
                return Futures.immediateFuture(task.call());
            }
            return defaultFuture;
        }
    }

    private static class TaskTypeMatcher extends TypeSafeMatcher<MetricsCollectingTaskDecorator> {

        final Matcher<TaskType> expectedTaskType;

        TaskTypeMatcher(TaskType expectedTaskType) {
            this(equalTo(expectedTaskType));
        }

        TaskTypeMatcher(Matcher<TaskType> expectedTaskType) {
            this.expectedTaskType = expectedTaskType;
        }

        @Override
        protected boolean matchesSafely(MetricsCollectingTaskDecorator item) {
            return expectedTaskType.matches(item.getTaskType());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("taskType matches");
            expectedTaskType.describeTo(description);
        }

        static TaskTypeMatcher isOfType(TaskType taskType) {
            return new TaskTypeMatcher(taskType);
        }

        static TaskTypeMatcher matchesType(Matcher<TaskType> matcher) {
            return new TaskTypeMatcher(matcher);
        }
    }

    private static class InnerTaskMatcher<T extends ITask> extends TypeSafeMatcher<MetricsCollectingTaskDecorator> {

        final Matcher<?> matcher;

        InnerTaskMatcher(Matcher<?> matcher) {
            this.matcher = matcher;
        }

        @Override
        protected boolean matchesSafely(MetricsCollectingTaskDecorator item) {
            return matcher.matches(item.getOther());
        }

        @Override
        public void describeTo(Description description) {
            matcher.describeTo(description);
        }

        static <U extends ITask> InnerTaskMatcher<U> taskWith(Class<U> clazz, Matcher<?> matcher) {
            return new InnerTaskMatcher<>(matcher);
        }
    }

    @RequiredArgsConstructor
    private static class ReflectionFieldMatcher<T extends ITask>
            extends TypeSafeDiagnosingMatcher<MetricsCollectingTaskDecorator> {

        private final Class<T> itemClass;
        private final String fieldName;
        private final Matcher<?> fieldMatcher;

        @Override
        protected boolean matchesSafely(MetricsCollectingTaskDecorator item, Description mismatchDescription) {
            if (item.getOther() == null) {
                mismatchDescription.appendText("inner task is null");
                return false;
            }
            ITask inner = item.getOther();
            if (!itemClass.equals(inner.getClass())) {
                mismatchDescription.appendText("inner task isn't an instance of ").appendText(itemClass.getName());
                return false;
            }
            try {
                Field field = itemClass.getDeclaredField(fieldName);
                field.setAccessible(true);
                if (!fieldMatcher.matches(field.get(inner))) {
                    mismatchDescription.appendText("Field '").appendText(fieldName).appendText("' doesn't match: ")
                            .appendDescriptionOf(fieldMatcher);
                    return false;
                }
                return true;
            } catch (NoSuchFieldException e) {
                mismatchDescription.appendText(itemClass.getName()).appendText(" doesn't have a field named ")
                        .appendText(fieldName);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            return false;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("An item of ").appendText(itemClass.getName()).appendText(" with the field '")
                    .appendText(fieldName).appendText("' matching ").appendDescriptionOf(fieldMatcher);
        }

        static <T extends ITask> ReflectionFieldMatcher<T> withField(Class<T> itemClass, String fieldName,
                Matcher<?> fieldMatcher) {
            return new ReflectionFieldMatcher<>(itemClass, fieldName, fieldMatcher);
        }
    }
    /**
     * Returns executor service that will be owned by the worker. This is useful to test the scenario
     * where worker shuts down the executor service also during shutdown flow.
     *
     * @return Executor service that will be owned by the worker.
     */
    private WorkerThreadPoolExecutor getWorkerThreadPoolExecutor() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("RecordProcessor-%04d").build();
        return new WorkerThreadPoolExecutor(threadFactory);
    }

    private List<Shard> createShardListWithOneShard() {
        List<Shard> shards = new ArrayList<Shard>();
        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("39428", "987324");
        HashKeyRange keyRange =
                ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, ShardObjectHelper.MAX_HASH_KEY);
        Shard shard0 = ShardObjectHelper.newShard("shardId-0", null, null, range0, keyRange);
        shards.add(shard0);

        return shards;
    }

    /**
     * @return
     */
    private List<Shard> createShardListWithOneSplit() {
        List<Shard> shards = new ArrayList<Shard>();
        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("39428", "987324");
        SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("987325", null);
        HashKeyRange keyRange =
                ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, ShardObjectHelper.MAX_HASH_KEY);
        Shard shard0 = ShardObjectHelper.newShard("shardId-0", null, null, range0, keyRange);
        shards.add(shard0);

        Shard shard1 = ShardObjectHelper.newShard("shardId-1", "shardId-0", null, range1, keyRange);
        shards.add(shard1);

        return shards;
    }

    private void runAndTestWorker(int numShards, int threadPoolSize) throws Exception {
        final int numberOfRecordsPerShard = 10;
        final String kinesisShardPrefix = "kinesis-0-";
        final BigInteger startSeqNum = BigInteger.ONE;
        List<Shard> shardList = KinesisLocalFileDataCreator.createShardList(numShards, kinesisShardPrefix, startSeqNum);
        Assert.assertEquals(numShards, shardList.size());
        List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        for (Shard shard : shardList) {
            KinesisClientLease lease = ShardSyncer.newKCLLease(shard);
            lease.setCheckpoint(ExtendedSequenceNumber.AT_TIMESTAMP);
            initialLeases.add(lease);
        }
        runAndTestWorker(shardList, threadPoolSize, initialLeases, callProcessRecordsForEmptyRecordList, numberOfRecordsPerShard, config);
    }

    private void runAndTestWorker(List<Shard> shardList,
                                  int threadPoolSize,
                                  List<KinesisClientLease> initialLeases,
                                  boolean callProcessRecordsForEmptyRecordList,
                                  int numberOfRecordsPerShard,
                                  KinesisClientLibConfiguration clientConfig) throws Exception {
        File file = KinesisLocalFileDataCreator.generateTempDataFile(shardList, numberOfRecordsPerShard, "unitTestWT001");
        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        Semaphore recordCounter = new Semaphore(0);
        ShardSequenceVerifier shardSequenceVerifier = new ShardSequenceVerifier(shardList);
        TestStreamletFactory recordProcessorFactory = new TestStreamletFactory(recordCounter, shardSequenceVerifier);

        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
        
        WorkerThread workerThread = runWorker(
                shardList, initialLeases, callProcessRecordsForEmptyRecordList, failoverTimeMillis,
                numberOfRecordsPerShard, fileBasedProxy, recordProcessorFactory, executorService, nullMetricsFactory, clientConfig);

        // TestStreamlet will release the semaphore once for every record it processes
        recordCounter.acquire(numberOfRecordsPerShard * shardList.size());

        // Wait a bit to allow the worker to spin against the end of the stream.
        Thread.sleep(500L);

        testWorker(shardList, threadPoolSize, initialLeases, callProcessRecordsForEmptyRecordList,
                numberOfRecordsPerShard, fileBasedProxy, recordProcessorFactory);

        workerThread.getWorker().shutdown();
        executorService.shutdownNow();
        file.delete();
    }

    private WorkerThread runWorker(List<Shard> shardList,
                                   List<KinesisClientLease> initialLeases,
                                   boolean callProcessRecordsForEmptyRecordList,
                                   long failoverTimeMillis,
                                   int numberOfRecordsPerShard,
                                   IKinesisProxy kinesisProxy,
                                   IRecordProcessorFactory recordProcessorFactory,
                                   ExecutorService executorService,
                                   IMetricsFactory metricsFactory,
                                   KinesisClientLibConfiguration clientConfig) throws Exception {
        final String stageName = "testStageName";
        final int maxRecords = 2;

        final long leaseDurationMillis = 10000L;
        final long epsilonMillis = 1000L;
        final long idleTimeInMilliseconds = 2L;

        AmazonDynamoDB ddbClient = DynamoDBEmbedded.create().amazonDynamoDB();
        LeaseManager<KinesisClientLease> leaseManager = new KinesisClientLeaseManager("foo", ddbClient);
        leaseManager.createLeaseTableIfNotExists(1L, 1L);
        for (KinesisClientLease initialLease : initialLeases) {
            leaseManager.createLeaseIfNotExists(initialLease);
        }

        KinesisClientLibLeaseCoordinator leaseCoordinator =
                new KinesisClientLibLeaseCoordinator(leaseManager,
                        stageName,
                        leaseDurationMillis,
                        epsilonMillis,
                        metricsFactory);

        final Date timestamp = new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP);
        StreamConfig streamConfig = new StreamConfig(kinesisProxy,
                maxRecords,
                idleTimeInMilliseconds,
                callProcessRecordsForEmptyRecordList,
                skipCheckpointValidationValue, InitialPositionInStreamExtended.newInitialPositionAtTimestamp(timestamp));
        
        Worker worker =
                new Worker(stageName,
                        recordProcessorFactory,
                        clientConfig,
                        streamConfig, INITIAL_POSITION_TRIM_HORIZON,
                        parentShardPollIntervalMillis,
                        shardSyncIntervalMillis,
                        cleanupLeasesUponShardCompletion,
                        leaseCoordinator,
                        leaseCoordinator,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        failoverTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        shardPrioritization);
        
        WorkerThread workerThread = new WorkerThread(worker);
        workerThread.start();
        return workerThread;
    }

    private void testWorker(List<Shard> shardList,
            int threadPoolSize,
            List<KinesisClientLease> initialLeases,
            boolean callProcessRecordsForEmptyRecordList,
            int numberOfRecordsPerShard,
            IKinesisProxy kinesisProxy,
            TestStreamletFactory recordProcessorFactory) throws Exception {
        recordProcessorFactory.getShardSequenceVerifier().verify();

        // Gather values to compare across all processors of a given shard.
        Map<String, List<Record>> shardStreamletsRecords = new HashMap<String, List<Record>>();
        Map<String, ShutdownReason> shardsLastProcessorShutdownReason = new HashMap<String, ShutdownReason>();
        Map<String, Long> shardsNumProcessRecordsCallsWithEmptyRecordList = new HashMap<String, Long>();
        for (TestStreamlet processor : recordProcessorFactory.getTestStreamlets()) {
            String shardId = processor.getShardId();
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
            shardsLastProcessorShutdownReason.put(processor.getShardId(), processor.getShutdownReason());
        }

        // verify that all records were processed at least once
        verifyAllRecordsOfEachShardWereConsumedAtLeastOnce(shardList, kinesisProxy, numberOfRecordsPerShard, shardStreamletsRecords);

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

    // within a record processor all the incoming records should be ordered
    private void verifyRecordsProcessedByEachProcessorWereOrdered(TestStreamletFactory recordProcessorFactory) {
        for (TestStreamlet processor : recordProcessorFactory.getTestStreamlets()) {
            List<Record> processedRecords = processor.getProcessedRecords();
            for (int i = 0; i < processedRecords.size() - 1; i++) {
                BigInteger sequenceNumberOfcurrentRecord = new BigInteger(processedRecords.get(i).getSequenceNumber());
                BigInteger sequenceNumberOfNextRecord = new BigInteger(processedRecords.get(i + 1).getSequenceNumber());
                Assert.assertTrue(sequenceNumberOfcurrentRecord.subtract(sequenceNumberOfNextRecord).signum() == -1);
            }
        }
    }

    // for shards for which only one record processor was created, we verify that each record should be
    // processed exactly once
    private void verifyAllRecordsOfEachShardWithOnlyOneProcessorWereConsumedExactlyOnce(List<Shard> shardList,
            IKinesisProxy fileBasedProxy,
            int numRecs,
            Map<String, List<Record>> shardStreamletsRecords,
            TestStreamletFactory recordProcessorFactory) {
        Map<String, TestStreamlet> shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor =
                findShardIdsAndStreamLetsOfShardsWithOnlyOneProcessor(recordProcessorFactory);
        for (Shard shard : shardList) {
            String shardId = shard.getShardId();
            String iterator =
                    fileBasedProxy.getIterator(shardId, new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP));
            List<Record> expectedRecords = fileBasedProxy.get(iterator, numRecs).getRecords();
            if (shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor.containsKey(shardId)) {
                verifyAllRecordsWereConsumedExactlyOnce(expectedRecords,
                        shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor.get(shardId).getProcessedRecords());
            }
        }
    }

    // verify that all records were processed at least once
    private void verifyAllRecordsOfEachShardWereConsumedAtLeastOnce(List<Shard> shardList,
            IKinesisProxy fileBasedProxy,
            int numRecs,
            Map<String, List<Record>> shardStreamletsRecords) {
        for (Shard shard : shardList) {
            String shardId = shard.getShardId();
            String iterator =
                    fileBasedProxy.getIterator(shardId, new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP));
            List<Record> expectedRecords = fileBasedProxy.get(iterator, numRecs).getRecords();
            verifyAllRecordsWereConsumedAtLeastOnce(expectedRecords, shardStreamletsRecords.get(shardId));
        }

    }

    // verify that worker shutdown last processor of shards that were terminated
    private void verifyLastProcessorOfClosedShardsWasShutdownWithTerminate(List<Shard> shardList,
            Map<String, ShutdownReason> shardsLastProcessorShutdownReason) {
        for (Shard shard : shardList) {
            String shardId = shard.getShardId();
            String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
            if (endingSequenceNumber != null) {
                LOG.info("Closed shard " + shardId + " has an endingSequenceNumber " + endingSequenceNumber);
                Assert.assertEquals(ShutdownReason.TERMINATE, shardsLastProcessorShutdownReason.get(shardId));
            }
        }
    }

    // if callProcessRecordsForEmptyRecordList flag is set then processors must have been invoked with empty record
    // sets else they shouldn't have seen invoked with empty record sets
    private void verifyNumProcessRecordsCallsWithEmptyRecordList(List<Shard> shardList,
            Map<String, Long> shardsNumProcessRecordsCallsWithEmptyRecordList,
            boolean callProcessRecordsForEmptyRecordList) {
        for (Shard shard : shardList) {
            String shardId = shard.getShardId();
            String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
            // check only for open shards
            if (endingSequenceNumber == null) {
                if (callProcessRecordsForEmptyRecordList) {
                    Assert.assertTrue(shardsNumProcessRecordsCallsWithEmptyRecordList.get(shardId) > 0);
                } else {
                    Assert.assertEquals(0, (long) shardsNumProcessRecordsCallsWithEmptyRecordList.get(shardId));
                }
            }
        }
    }

    private Map<String, TestStreamlet>
            findShardIdsAndStreamLetsOfShardsWithOnlyOneProcessor(TestStreamletFactory recordProcessorFactory) {
        Map<String, TestStreamlet> shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor =
                new HashMap<String, TestStreamlet>();
        Set<String> seenShardIds = new HashSet<String>();
        for (TestStreamlet processor : recordProcessorFactory.getTestStreamlets()) {
            String shardId = processor.getShardId();
            if (seenShardIds.add(shardId)) {
                shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor.put(shardId, processor);
            } else {
                shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor.remove(shardId);
            }
        }
        return shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor;
    }

    //@formatter:off (gets the formatting wrong)
    private void verifyAllRecordsWereConsumedExactlyOnce(List<Record> expectedRecords,
            List<Record> actualRecords) {
        //@formatter:on
        Assert.assertEquals(expectedRecords.size(), actualRecords.size());
        ListIterator<Record> expectedIter = expectedRecords.listIterator();
        ListIterator<Record> actualIter = actualRecords.listIterator();
        for (int i = 0; i < expectedRecords.size(); ++i) {
            Assert.assertEquals(expectedIter.next(), actualIter.next());
        }
    }

    //@formatter:off (gets the formatting wrong)
    private void verifyAllRecordsWereConsumedAtLeastOnce(List<Record> expectedRecords,
            List<Record> actualRecords) {
        //@formatter:on
        ListIterator<Record> expectedIter = expectedRecords.listIterator();
        for (int i = 0; i < expectedRecords.size(); ++i) {
            Record expectedRecord = expectedIter.next();
            Assert.assertTrue(actualRecords.contains(expectedRecord));
        }
    }

    private static class WorkerThread extends Thread {
        private final Worker worker;

        private WorkerThread(Worker worker) {
            super(worker);
            this.worker = worker;
        }

        public Worker getWorker() {
            return worker;
        }
    }
}
