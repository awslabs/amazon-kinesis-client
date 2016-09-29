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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.InMemoryCheckpointImpl;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardConsumer.ShardConsumerState;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisLocalFileProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.util.KinesisLocalFileDataCreator;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * Unit tests of {@link ShardConsumer}.
 */
public class ShardConsumerTest {

    private static final Log LOG = LogFactory.getLog(ShardConsumerTest.class);

    private final IMetricsFactory metricsFactory = new NullMetricsFactory();
    private final boolean callProcessRecordsForEmptyRecordList = false;
    private final long taskBackoffTimeMillis = 500L;
    private final long parentShardPollIntervalMillis = 50L;
    private final boolean cleanupLeasesOfCompletedShards = true;
    // We don't want any of these tests to run checkpoint validation
    private final boolean skipCheckpointValidationValue = false;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

    // Use Executors.newFixedThreadPool since it returns ThreadPoolExecutor, which is
    // ... a non-final public class, and so can be mocked and spied.
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    /**
     * Test method to verify consumer stays in INITIALIZING state when InitializationTask fails.
     */
    @SuppressWarnings("unchecked")
    @Test
    public final void testInitializationStateUponFailure() throws Exception {
        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.TRIM_HORIZON);
        ICheckpoint checkpoint = mock(ICheckpoint.class);

        when(checkpoint.getCheckpoint(anyString())).thenThrow(NullPointerException.class);
        IRecordProcessor processor = mock(IRecordProcessor.class);
        IKinesisProxy streamProxy = mock(IKinesisProxy.class);
        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        when(leaseManager.getLease(anyString())).thenReturn(null);
        StreamConfig streamConfig =
                new StreamConfig(streamProxy,
                        1,
                        10,
                        callProcessRecordsForEmptyRecordList,
                        skipCheckpointValidationValue, INITIAL_POSITION_LATEST);

        ShardConsumer consumer =
                new ShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        null,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis);

        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
    }


    /**
     * Test method to verify consumer stays in INITIALIZING state when InitializationTask fails.
     */
    @SuppressWarnings("unchecked")
    @Test
    public final void testInitializationStateUponSubmissionFailure() throws Exception {
        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.TRIM_HORIZON);
        ICheckpoint checkpoint = mock(ICheckpoint.class);
        ExecutorService spyExecutorService = spy(executorService);

        when(checkpoint.getCheckpoint(anyString())).thenThrow(NullPointerException.class);
        IRecordProcessor processor = mock(IRecordProcessor.class);
        IKinesisProxy streamProxy = mock(IKinesisProxy.class);
        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        when(leaseManager.getLease(anyString())).thenReturn(null);
        StreamConfig streamConfig =
                new StreamConfig(streamProxy,
                        1,
                        10,
                        callProcessRecordsForEmptyRecordList,
                        skipCheckpointValidationValue, INITIAL_POSITION_LATEST);

        ShardConsumer consumer =
                new ShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        null,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        spyExecutorService,
                        metricsFactory,
                        taskBackoffTimeMillis);

        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS)));

        doThrow(new RejectedExecutionException()).when(spyExecutorService).submit(any(InitializeTask.class));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public final void testRecordProcessorThrowable() throws Exception {
        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.TRIM_HORIZON);
        ICheckpoint checkpoint = mock(ICheckpoint.class);
        IRecordProcessor processor = mock(IRecordProcessor.class);
        IKinesisProxy streamProxy = mock(IKinesisProxy.class);
        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        StreamConfig streamConfig =
                new StreamConfig(streamProxy,
                        1,
                        10,
                        callProcessRecordsForEmptyRecordList,
                        skipCheckpointValidationValue, INITIAL_POSITION_LATEST);

        ShardConsumer consumer =
                new ShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        null,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis);

        when(leaseManager.getLease(anyString())).thenReturn(null);
        when(checkpoint.getCheckpoint(anyString())).thenReturn(new ExtendedSequenceNumber("123"));

        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // submit BlockOnParentShardTask
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        verify(processor, times(0)).initialize(any(InitializationInput.class));

        // Throw Error when IRecordProcessor.initialize() is invoked.
        doThrow(new Error("ThrowableTest")).when(processor).initialize(any(InitializationInput.class));

        consumer.consumeShard(); // submit InitializeTask
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        verify(processor, times(1)).initialize(any(InitializationInput.class));

        try {
            // Checking the status of submitted InitializeTask from above should throw exception.
            consumer.consumeShard();
            fail("ShardConsumer should have thrown exception.");
        } catch (RuntimeException e) {
            assertThat(e.getCause(), instanceOf(ExecutionException.class));
        }
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        verify(processor, times(1)).initialize(any(InitializationInput.class));

        doNothing().when(processor).initialize(any(InitializationInput.class));

        consumer.consumeShard(); // submit InitializeTask again.
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        verify(processor, times(2)).initialize(any(InitializationInput.class));

        // Checking the status of submitted InitializeTask from above should pass.
        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.PROCESSING)));
    }

    /**
     * Test method for {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardConsumer#consumeShard()}
     */
    @Test
    public final void testConsumeShard() throws Exception {
        int numRecs = 10;
        BigInteger startSeqNum = BigInteger.ONE;
        String streamShardId = "kinesis-0-0";
        String testConcurrencyToken = "testToken";
        File file =
                KinesisLocalFileDataCreator.generateTempDataFile(1,
                        "kinesis-0-",
                        numRecs,
                        startSeqNum,
                        "unitTestSCT001");

        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        final int maxRecords = 2;
        final int idleTimeMS = 0; // keep unit tests fast
        ICheckpoint checkpoint = new InMemoryCheckpointImpl(startSeqNum.toString());
        checkpoint.setCheckpoint(streamShardId, ExtendedSequenceNumber.TRIM_HORIZON, testConcurrencyToken);
        @SuppressWarnings("unchecked")
        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        when(leaseManager.getLease(anyString())).thenReturn(null);

        TestStreamlet processor = new TestStreamlet();

        StreamConfig streamConfig =
                new StreamConfig(fileBasedProxy,
                        maxRecords,
                        idleTimeMS,
                        callProcessRecordsForEmptyRecordList,
                        skipCheckpointValidationValue, INITIAL_POSITION_LATEST);

        ShardInfo shardInfo = new ShardInfo(streamShardId, testConcurrencyToken, null, null);
        ShardConsumer consumer =
                new ShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        leaseManager,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis);

        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // check on parent shards
        Thread.sleep(50L);
        consumer.consumeShard(); // start initialization
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);

        // We expect to process all records in numRecs calls
        for (int i = 0; i < numRecs;) {
            boolean newTaskSubmitted = consumer.consumeShard();
            if (newTaskSubmitted) {
                LOG.debug("New processing task was submitted, call # " + i);
                assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.PROCESSING)));
                // CHECKSTYLE:IGNORE ModifiedControlVariable FOR NEXT 1 LINES
                i += maxRecords;
            }
            Thread.sleep(50L);
        }

        assertThat(processor.getShutdownReason(), nullValue());
        consumer.beginShutdown();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.SHUTTING_DOWN)));
        consumer.beginShutdown();
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.SHUTDOWN_COMPLETE)));
        assertThat(processor.getShutdownReason(), is(equalTo(ShutdownReason.ZOMBIE)));

        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);

        String iterator = fileBasedProxy.getIterator(streamShardId, ShardIteratorType.TRIM_HORIZON.toString());
        List<Record> expectedRecords = toUserRecords(fileBasedProxy.get(iterator, numRecs).getRecords());
        verifyConsumedRecords(expectedRecords, processor.getProcessedRecords());
        file.delete();
    }

    /**
     * Test method for {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardConsumer#consumeShard()}
     * that starts from initial position of type AT_TIMESTAMP.
     */
    @Test
    public final void testConsumeShardWithInitialPositionAtTimestamp() throws Exception {
        int numRecs = 7;
        BigInteger startSeqNum = BigInteger.ONE;
        Date timestamp = new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP + 3);
        InitialPositionInStreamExtended atTimestamp =
                InitialPositionInStreamExtended.newInitialPositionAtTimestamp(timestamp);
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
        final int idleTimeMS = 0; // keep unit tests fast
        ICheckpoint checkpoint = new InMemoryCheckpointImpl(startSeqNum.toString());
        checkpoint.setCheckpoint(streamShardId, ExtendedSequenceNumber.AT_TIMESTAMP, testConcurrencyToken);
        @SuppressWarnings("unchecked")
        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        when(leaseManager.getLease(anyString())).thenReturn(null);

        TestStreamlet processor = new TestStreamlet();

        StreamConfig streamConfig =
                new StreamConfig(fileBasedProxy,
                        maxRecords,
                        idleTimeMS,
                        callProcessRecordsForEmptyRecordList,
                        skipCheckpointValidationValue,
                        atTimestamp);

        ShardInfo shardInfo = new ShardInfo(streamShardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardConsumer consumer =
                new ShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        leaseManager,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis);

        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // check on parent shards
        Thread.sleep(50L);
        consumer.consumeShard(); // start initialization
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);

        // We expect to process all records in numRecs calls
        for (int i = 0; i < numRecs;) {
            boolean newTaskSubmitted = consumer.consumeShard();
            if (newTaskSubmitted) {
                LOG.debug("New processing task was submitted, call # " + i);
                assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.PROCESSING)));
                // CHECKSTYLE:IGNORE ModifiedControlVariable FOR NEXT 1 LINES
                i += maxRecords;
            }
            Thread.sleep(50L);
        }

        assertThat(processor.getShutdownReason(), nullValue());
        consumer.beginShutdown();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.SHUTTING_DOWN)));
        consumer.beginShutdown();
        assertThat(consumer.getCurrentState(), is(equalTo(ShardConsumerState.SHUTDOWN_COMPLETE)));
        assertThat(processor.getShutdownReason(), is(equalTo(ShutdownReason.ZOMBIE)));

        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);

        String iterator = fileBasedProxy.getIterator(streamShardId, timestamp);
        List<Record> expectedRecords = toUserRecords(fileBasedProxy.get(iterator, numRecs).getRecords());
        verifyConsumedRecords(expectedRecords, processor.getProcessedRecords());
        assertEquals(4, processor.getProcessedRecords().size());
        file.delete();
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
}
