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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.InMemoryCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisLocalFileProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.util.KinesisLocalFileDataCreator;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.checkpoint.RecordProcessorCheckpointer;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.retrieval.AsynchronousGetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.BlockingGetRecordsCache;
import software.amazon.kinesis.retrieval.GetRecordsCache;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.IKinesisProxy;
import software.amazon.kinesis.retrieval.KinesisDataFetcher;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.SimpleRecordsFetcherFactory;
import software.amazon.kinesis.retrieval.SynchronousGetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.retrieval.kpl.UserRecord;
import software.amazon.kinesis.utils.TestStreamlet;

/**
 * Unit tests of {@link ShardConsumer}.
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
@Ignore
public class ShardConsumerTest {
    private final IMetricsFactory metricsFactory = new NullMetricsFactory();
    private final boolean callProcessRecordsForEmptyRecordList = false;
    private final long taskBackoffTimeMillis = 500L;
    private final long parentShardPollIntervalMillis = 50L;
    private final InitialPositionInStreamExtended initialPositionLatest =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private final boolean cleanupLeasesOfCompletedShards = true;
    // We don't want any of these tests to run checkpoint validation
    private final boolean skipCheckpointValidationValue = false;
    private final long listShardsBackoffTimeInMillis = 500L;
    private final int maxListShardRetryAttempts = 50;
    private final long idleTimeInMillis = 500L;
    private final boolean ignoreUnexpectedChildShards = false;
    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist = false;
    private final String streamName = "TestStream";
    private final String shardId = "shardId-0-0";
    private final String concurrencyToken = "TestToken";
    private final int maxRecords = 2;
    private ShardInfo shardInfo;

    // Use Executors.newFixedThreadPool since it returns ThreadPoolExecutor, which is
    // ... a non-final public class, and so can be mocked and spied.
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);


    private GetRecordsCache getRecordsCache;
    
    private KinesisDataFetcher dataFetcher;

    @Mock
    private RecordsFetcherFactory recordsFetcherFactory;
    @Mock
    private RecordProcessor recordProcessor;
    @Mock
    private KinesisClientLibConfiguration config;
    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private Checkpointer checkpoint;
    @Mock
    private ShutdownNotification shutdownNotification;
    @Mock
    private AmazonKinesis amazonKinesis;
    @Mock
    private RecordProcessorCheckpointer recordProcessorCheckpointer;
    @Mock
    private ShardDetector shardDetector;

    @Before
    public void setup() {
        shardInfo = new ShardInfo(shardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        dataFetcher = new KinesisDataFetcher(amazonKinesis, streamName, shardId, maxRecords);
        getRecordsCache = new BlockingGetRecordsCache(maxRecords,
                new SynchronousGetRecordsRetrievalStrategy(dataFetcher));

        //recordsFetcherFactory = spy(new SimpleRecordsFetcherFactory());
        when(config.getRecordsFetcherFactory()).thenReturn(recordsFetcherFactory);
        when(config.getLogWarningForTaskAfterMillis()).thenReturn(Optional.empty());
    }
    
    /**
     * Test method to verify consumer stays in INITIALIZING state when InitializationTask fails.
     */
    @SuppressWarnings("unchecked")
    @Test
//    TODO: check if sleeps can be removed
    public final void testInitializationStateUponFailure() throws Exception {
        when(checkpoint.getCheckpoint(eq(shardId))).thenThrow(NullPointerException.class);
        when(checkpoint.getCheckpointObject(eq(shardId))).thenThrow(NullPointerException.class);

        final ShardConsumer consumer = createShardConsumer(shardInfo, executorService, Optional.empty());
        
        assertEquals(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS, consumer.getCurrentState());
        consumer.consumeShard(); // initialize
        assertEquals(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS, consumer.getCurrentState());
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertEquals(ConsumerStates.ShardConsumerState.INITIALIZING, consumer.getCurrentState());
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertEquals(ConsumerStates.ShardConsumerState.INITIALIZING, consumer.getCurrentState());
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertEquals(ConsumerStates.ShardConsumerState.INITIALIZING, consumer.getCurrentState());
    }

    /**
     * Test method to verify consumer stays in INITIALIZING state when InitializationTask fails.
     */
    @SuppressWarnings("unchecked")
    @Test
    public final void testInitializationStateUponSubmissionFailure() throws Exception {
        final ExecutorService spyExecutorService = spy(executorService);

        when(checkpoint.getCheckpoint(anyString())).thenThrow(NullPointerException.class);
        when(checkpoint.getCheckpointObject(anyString())).thenThrow(NullPointerException.class);

        final ShardConsumer consumer = createShardConsumer(shardInfo, spyExecutorService, Optional.empty());

        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // initialize
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));

        doThrow(new RejectedExecutionException()).when(spyExecutorService).submit(any(InitializeTask.class));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public final void testRecordProcessorThrowable() throws Exception {
        ShardConsumer consumer = createShardConsumer(shardInfo, executorService, Optional.empty());

        final ExtendedSequenceNumber checkpointSequenceNumber = new ExtendedSequenceNumber("123");
        final ExtendedSequenceNumber pendingCheckpointSequenceNumber = null;
        when(leaseRefresher.getLease(anyString())).thenReturn(null);
        when(checkpoint.getCheckpointObject(anyString())).thenReturn(
                new Checkpoint(checkpointSequenceNumber, pendingCheckpointSequenceNumber));

        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // submit BlockOnParentShardTask
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
//        verify(recordProcessor, times(0)).initialize(any(InitializationInput.class));

        // Throw Error when IRecordProcessor.initialize() is invoked.
        doThrow(new Error("ThrowableTest")).when(recordProcessor).initialize(any(InitializationInput.class));

        consumer.consumeShard(); // submit InitializeTask
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        verify(recordProcessor, times(1)).initialize(argThat(
                initializationInputMatcher(checkpointSequenceNumber, pendingCheckpointSequenceNumber)));

        try {
            // Checking the status of submitted InitializeTask from above should throw exception.
            consumer.consumeShard();
            fail("ShardConsumer should have thrown exception.");
        } catch (RuntimeException e) {
            assertThat(e.getCause(), instanceOf(ExecutionException.class));
        }
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        verify(recordProcessor, times(1)).initialize(argThat(
                initializationInputMatcher(checkpointSequenceNumber, pendingCheckpointSequenceNumber)));

        doNothing().when(recordProcessor).initialize(any(InitializationInput.class));

        consumer.consumeShard(); // submit InitializeTask again.
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        verify(recordProcessor, times(2)).initialize(argThat(
                initializationInputMatcher(checkpointSequenceNumber, pendingCheckpointSequenceNumber)));
        verify(recordProcessor, times(2)).initialize(any(InitializationInput.class)); // no other calls with different args

        // Checking the status of submitted InitializeTask from above should pass.
        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.PROCESSING)));
    }

    /**
     * Test method for {@link ShardConsumer#consumeShard()}
     */
    @Test
    public final void testConsumeShard() throws Exception {
        int numRecs = 10;
        BigInteger startSeqNum = BigInteger.ONE;
        File file =
                KinesisLocalFileDataCreator.generateTempDataFile(1,
                        "kinesis-0-",
                        numRecs,
                        startSeqNum,
                        "unitTestSCT001");

        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        final int maxRecords = 2;
        Checkpointer checkpoint = new InMemoryCheckpointer(startSeqNum.toString());
        checkpoint.setCheckpoint(shardId, ExtendedSequenceNumber.TRIM_HORIZON, concurrencyToken);
        when(leaseRefresher.getLease(anyString())).thenReturn(null);
        TestStreamlet processor = new TestStreamlet();
        shardInfo = new ShardInfo(shardId, concurrencyToken, null, null);

        when(recordsFetcherFactory.createRecordsFetcher(any(GetRecordsRetrievalStrategy.class), anyString(),
                any(IMetricsFactory.class), anyInt()))
                .thenReturn(getRecordsCache);
        
        ShardConsumer consumer = createShardConsumer(shardInfo, executorService, Optional.empty());

        consumer.consumeShard(); // check on parent shards

        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // check on parent shards
        Thread.sleep(50L);
        consumer.consumeShard(); // start initialization
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        processor.getInitializeLatch().await(5, TimeUnit.SECONDS);
        verify(getRecordsCache).start(any(ExtendedSequenceNumber.class), any(InitialPositionInStreamExtended.class));

        // We expect to process all records in numRecs calls
        for (int i = 0; i < numRecs;) {
            boolean newTaskSubmitted = consumer.consumeShard();
            if (newTaskSubmitted) {
                log.debug("New processing task was submitted, call # {}", i);
                assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.PROCESSING)));
                // CHECKSTYLE:IGNORE ModifiedControlVariable FOR NEXT 1 LINES
                i += maxRecords;
            }
            Thread.sleep(50L);
        }
        
        verify(getRecordsCache, times(5)).getNextResult();

        assertThat(processor.getShutdownReason(), nullValue());
        consumer.notifyShutdownRequested(shutdownNotification);
        consumer.consumeShard();
        assertThat(processor.getNotifyShutdownLatch().await(1, TimeUnit.SECONDS), is(true));
        Thread.sleep(50);
        assertThat(consumer.shutdownReason(), equalTo(ShutdownReason.REQUESTED));
        assertThat(consumer.getCurrentState(), equalTo(ConsumerStates.ShardConsumerState.SHUTDOWN_REQUESTED));
        verify(shutdownNotification).shutdownNotificationComplete();
        assertThat(processor.isShutdownNotificationCalled(), equalTo(true));
        consumer.consumeShard();
        Thread.sleep(50);
        assertThat(consumer.getCurrentState(), equalTo(ConsumerStates.ShardConsumerState.SHUTDOWN_REQUESTED));

        consumer.beginShutdown();
        Thread.sleep(50L);
        assertThat(consumer.shutdownReason(), equalTo(ShutdownReason.ZOMBIE));
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.SHUTTING_DOWN)));
        consumer.beginShutdown();
        consumer.consumeShard();
        verify(shutdownNotification, atLeastOnce()).shutdownComplete();
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE)));
        assertThat(processor.getShutdownReason(), is(equalTo(ShutdownReason.ZOMBIE)));
        
        verify(getRecordsCache).shutdown();

        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);

        String iterator = fileBasedProxy.getIterator(shardId, ShardIteratorType.TRIM_HORIZON.toString());
        List<Record> expectedRecords = toUserRecords(fileBasedProxy.get(iterator, numRecs).getRecords());
        verifyConsumedRecords(expectedRecords, processor.getProcessedRecords());
        file.delete();
    }

    private static final class TransientShutdownErrorTestStreamlet extends TestStreamlet {
        private final CountDownLatch errorShutdownLatch = new CountDownLatch(1);

        @Override
        public void shutdown(ShutdownInput input) {
            ShutdownReason reason = input.shutdownReason();
            if (reason.equals(ShutdownReason.TERMINATE) && errorShutdownLatch.getCount() > 0) {
                errorShutdownLatch.countDown();
                throw new RuntimeException("test");
            } else {
                super.shutdown(input);
            }
        }
    }

    /**
     * Test method for {@link ShardConsumer#consumeShard()} that ensures a transient error thrown from the record
     * recordProcessor's shutdown method with reason terminate will be retried.
     */
    @Test
    public final void testConsumeShardWithTransientTerminateError() throws Exception {
        int numRecs = 10;
        BigInteger startSeqNum = BigInteger.ONE;
        List<Shard> shardList = KinesisLocalFileDataCreator.createShardList(1, "kinesis-0-", startSeqNum);
        // Close the shard so that shutdown is called with reason terminate
        shardList.get(0).getSequenceNumberRange().setEndingSequenceNumber(
                KinesisLocalFileProxy.MAX_SEQUENCE_NUMBER.subtract(BigInteger.ONE).toString());
        File file = KinesisLocalFileDataCreator.generateTempDataFile(shardList, numRecs, "unitTestSCT002");

        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        final int maxRecords = 2;
        Checkpointer checkpoint = new InMemoryCheckpointer(startSeqNum.toString());
        checkpoint.setCheckpoint(shardId, ExtendedSequenceNumber.TRIM_HORIZON, concurrencyToken);
        when(leaseRefresher.getLease(anyString())).thenReturn(null);

        TransientShutdownErrorTestStreamlet processor = new TransientShutdownErrorTestStreamlet();
        shardInfo = new ShardInfo(shardId, concurrencyToken, null, null);

        when(recordsFetcherFactory.createRecordsFetcher(any(GetRecordsRetrievalStrategy.class), anyString(),
                any(IMetricsFactory.class), anyInt()))
                .thenReturn(getRecordsCache);

        ShardConsumer consumer = createShardConsumer(shardInfo, executorService, Optional.empty());

        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // check on parent shards
        Thread.sleep(50L);
        consumer.consumeShard(); // start initialization
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        processor.getInitializeLatch().await(5, TimeUnit.SECONDS);
        verify(getRecordsCache).start(any(ExtendedSequenceNumber.class), any(InitialPositionInStreamExtended.class));

        // We expect to process all records in numRecs calls
        for (int i = 0; i < numRecs;) {
            boolean newTaskSubmitted = consumer.consumeShard();
            if (newTaskSubmitted) {
                log.debug("New processing task was submitted, call # {}", i);
                assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.PROCESSING)));
                // CHECKSTYLE:IGNORE ModifiedControlVariable FOR NEXT 1 LINES
                i += maxRecords;
            }
            Thread.sleep(50L);
        }

        // Consume shards until shutdown terminate is called and it has thrown an exception
        for (int i = 0; i < 100; i++) {
            consumer.consumeShard();
            if (processor.errorShutdownLatch.await(50, TimeUnit.MILLISECONDS)) {
                break;
            }
        }
        assertEquals(0, processor.errorShutdownLatch.getCount());

        // Wait for a retry of shutdown terminate that should succeed
        for (int i = 0; i < 100; i++) {
            consumer.consumeShard();
            if (processor.getShutdownLatch().await(50, TimeUnit.MILLISECONDS)) {
                break;
            }
        }
        assertEquals(0, processor.getShutdownLatch().getCount());

        // Wait for shutdown complete now that terminate shutdown is successful
        for (int i = 0; i < 100; i++) {
            consumer.consumeShard();
            if (consumer.getCurrentState() == ConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE) {
                break;
            }
            Thread.sleep(50L);
        }
        assertThat(consumer.getCurrentState(), equalTo(ConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE));

        assertThat(processor.getShutdownReason(), is(equalTo(ShutdownReason.TERMINATE)));

        verify(getRecordsCache).shutdown();

        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);

        String iterator = fileBasedProxy.getIterator(shardId, ShardIteratorType.TRIM_HORIZON.toString());
        List<Record> expectedRecords = toUserRecords(fileBasedProxy.get(iterator, numRecs).getRecords());
        verifyConsumedRecords(expectedRecords, processor.getProcessedRecords());
        file.delete();
    }

    /**
     * Test method for {@link ShardConsumer#consumeShard()} that starts from initial position of type AT_TIMESTAMP.
     */
    @Test
    public final void testConsumeShardWithInitialPositionAtTimestamp() throws Exception {
        int numRecs = 7;
        BigInteger startSeqNum = BigInteger.ONE;
        Date timestamp = new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP + 3);
        String streamShardId = "kinesis-0-0";
        String testConcurrencyToken = "testToken";
        File file =
                KinesisLocalFileDataCreator.generateTempDataFile(1,
                        "kinesis-0-",
                        numRecs,
                        startSeqNum,
                        "unitTestSCT002");

        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        final int maxRecords = 2;
        Checkpointer checkpoint = new InMemoryCheckpointer(startSeqNum.toString());
        checkpoint.setCheckpoint(streamShardId, ExtendedSequenceNumber.AT_TIMESTAMP, testConcurrencyToken);
        when(leaseRefresher.getLease(anyString())).thenReturn(null);
        TestStreamlet processor = new TestStreamlet();

        when(recordsFetcherFactory.createRecordsFetcher(any(GetRecordsRetrievalStrategy.class), anyString(),
                any(IMetricsFactory.class), anyInt()))
                .thenReturn(getRecordsCache);

        ShardConsumer consumer = createShardConsumer(shardInfo, executorService, Optional.empty());

        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // check on parent shards
        Thread.sleep(50L);
        consumer.consumeShard(); // start initialization
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(200L);
        
//        verify(getRecordsCache).start();

        // We expect to process all records in numRecs calls
        for (int i = 0; i < numRecs;) {
            boolean newTaskSubmitted = consumer.consumeShard();
            if (newTaskSubmitted) {
                log.debug("New processing task was submitted, call # {}", i);
                assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.PROCESSING)));
                // CHECKSTYLE:IGNORE ModifiedControlVariable FOR NEXT 1 LINES
                i += maxRecords;
            }
            Thread.sleep(50L);
        }
        
//        verify(getRecordsCache, times(4)).getNextResult();

        assertThat(processor.getShutdownReason(), nullValue());
        consumer.beginShutdown();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.SHUTTING_DOWN)));
        consumer.beginShutdown();
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE)));
        assertThat(processor.getShutdownReason(), is(equalTo(ShutdownReason.ZOMBIE)));

        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);

        verify(getRecordsCache).shutdown();

        String iterator = fileBasedProxy.getIterator(streamShardId, timestamp);
        List<Record> expectedRecords = toUserRecords(fileBasedProxy.get(iterator, numRecs).getRecords());
        
        verifyConsumedRecords(expectedRecords, processor.getProcessedRecords());
        assertEquals(4, processor.getProcessedRecords().size());
        file.delete();
    }

    @SuppressWarnings("unchecked")
    @Test
    public final void testConsumeShardInitializedWithPendingCheckpoint() throws Exception {
        ShardConsumer consumer = createShardConsumer(shardInfo, executorService, Optional.empty());

        final ExtendedSequenceNumber checkpointSequenceNumber = new ExtendedSequenceNumber("123");
        final ExtendedSequenceNumber pendingCheckpointSequenceNumber = new ExtendedSequenceNumber("999");
        when(leaseRefresher.getLease(anyString())).thenReturn(null);
        when(config.getRecordsFetcherFactory()).thenReturn(new SimpleRecordsFetcherFactory());
        when(checkpoint.getCheckpointObject(anyString())).thenReturn(
                new Checkpoint(checkpointSequenceNumber, pendingCheckpointSequenceNumber));

        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // submit BlockOnParentShardTask
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
//        verify(recordProcessor, times(0)).initialize(any(InitializationInput.class));

        consumer.consumeShard(); // submit InitializeTask
        Thread.sleep(1L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.INITIALIZING)));
        verify(recordProcessor, times(1)).initialize(argThat(
                initializationInputMatcher(checkpointSequenceNumber, pendingCheckpointSequenceNumber)));
        verify(recordProcessor, times(1)).initialize(any(InitializationInput.class)); // no other calls with different args

        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ConsumerStates.ShardConsumerState.PROCESSING)));
    }
    
    @Test
    public void testCreateSynchronousGetRecordsRetrieval() {
        ShardConsumer consumer = createShardConsumer(shardInfo, executorService, Optional.empty());
        
        assertEquals(consumer.getRecordsCache().getGetRecordsRetrievalStrategy().getClass(),
                SynchronousGetRecordsRetrievalStrategy.class);
    }

    @Test
    public void testCreateAsynchronousGetRecordsRetrieval() {
        getRecordsCache = new BlockingGetRecordsCache(maxRecords,
                new AsynchronousGetRecordsRetrievalStrategy(dataFetcher, 5, 3, shardId));
        ShardConsumer consumer = createShardConsumer(shardInfo, executorService, Optional.empty());

        assertEquals(consumer.getRecordsCache().getGetRecordsRetrievalStrategy().getClass(),
                AsynchronousGetRecordsRetrievalStrategy.class);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testLongRunningTasks() throws InterruptedException {
        final long sleepTime = 1000L;
        ExecutorService mockExecutorService = mock(ExecutorService.class);
        Future<TaskResult> mockFuture = mock(Future.class);
        
        when(mockExecutorService.submit(any(ITask.class))).thenReturn(mockFuture);
        when(mockFuture.isDone()).thenReturn(false);
        when(mockFuture.isCancelled()).thenReturn(false);
        
        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.LATEST);

        ShardConsumer shardConsumer = spy(createShardConsumer(shardInfo, mockExecutorService, Optional.of(sleepTime)));
        
        shardConsumer.consumeShard();

        Thread.sleep(sleepTime);
        
        shardConsumer.consumeShard();
        
        verify(shardConsumer, times(2)).logWarningForTaskAfterMillis();
        verify(mockFuture).isDone();
        verify(mockFuture).isCancelled();
    }

    //@formatter:off (gets the formatting wrong)
    private void verifyConsumedRecords(List<Record> expectedRecords,
            List<Record> actualRecords) {
        //@formatter:on
        assertThat(actualRecords.size(), is(equalTo(expectedRecords.size())));
        ListIterator<Record> expectedIter = expectedRecords.listIterator();
        ListIterator<Record> actualIter = actualRecords.listIterator();
        for (int i = 0; i < expectedRecords.size(); ++i) {
            assertThat(actualIter.next(), is(equalTo(expectedIter.next())));
        }
    }

    private List<Record> toUserRecords(List<Record> records) {
        if (records == null || records.isEmpty()) {
            return records;
        }
        List<Record> userRecords = new ArrayList<Record>();
        for (Record record : records) {
            userRecords.add(new UserRecord(record));
        }
        return userRecords;
    }

    private ShardConsumer createShardConsumer(final ShardInfo shardInfo,
                                              final ExecutorService executorService,
                                              final Optional<Long> logWarningForTaskAfterMillis) {
        return new ShardConsumer(shardInfo,
                streamName,
                leaseRefresher,
                executorService,
                getRecordsCache,
                recordProcessor,
                checkpoint,
                recordProcessorCheckpointer,
                parentShardPollIntervalMillis,
                taskBackoffTimeMillis,
                logWarningForTaskAfterMillis,
                amazonKinesis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                listShardsBackoffTimeInMillis,
                maxListShardRetryAttempts,
                callProcessRecordsForEmptyRecordList,
                idleTimeInMillis,
                initialPositionLatest,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                shardDetector,
                metricsFactory);
    }

    Matcher<InitializationInput> initializationInputMatcher(final ExtendedSequenceNumber checkpoint,
                                                            final ExtendedSequenceNumber pendingCheckpoint) {
        return new TypeSafeMatcher<InitializationInput>() {
            @Override
            protected boolean matchesSafely(InitializationInput item) {
                return Objects.equals(checkpoint, item.getExtendedSequenceNumber())
                        && Objects.equals(pendingCheckpoint, item.getPendingCheckpointSequenceNumber());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(String.format("Checkpoint should be %s and pending checkpoint should be %s",
                        checkpoint, pendingCheckpoint));
            }
        };
    }
}
