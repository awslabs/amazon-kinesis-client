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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibNonRetryableException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

/**
 * Streamlet that tracks records it's seen - useful for testing.
 */
class TestStreamlet implements IRecordProcessor, IShutdownNotificationAware {

    private static final Log LOG = LogFactory.getLog(TestStreamlet.class);

    private List<Record> records = new ArrayList<Record>();

    private Set<String> processedSeqNums = new HashSet<String>(); // used for deduping

    private Semaphore sem; // used to allow test cases to wait for all records to be processed

    private String shardId;

    // record the last shutdown reason we were called with.
    private ShutdownReason shutdownReason;
    private ShardSequenceVerifier shardSequenceVerifier;
    private long numProcessRecordsCallsWithEmptyRecordList;
    private boolean shutdownNotificationCalled;

    private final CountDownLatch initializeLatch = new CountDownLatch(1);
    private final CountDownLatch notifyShutdownLatch = new CountDownLatch(1);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public TestStreamlet() {

    }

    public TestStreamlet(Semaphore sem, ShardSequenceVerifier shardSequenceVerifier) {
        this();
        this.sem = sem;
        this.shardSequenceVerifier = shardSequenceVerifier;
    }

    public List<Record> getProcessedRecords() {
        return records;
    }

    @Override
    public void initialize(InitializationInput input) {
        shardId = input.getShardId();
        if (shardSequenceVerifier != null) {
            shardSequenceVerifier.registerInitialization(shardId);
        }
        initializeLatch.countDown();
    }

    @Override
    public void processRecords(ProcessRecordsInput input) {
        List<Record> dataRecords = input.getRecords();
        IRecordProcessorCheckpointer checkpointer = input.getCheckpointer();
        if ((dataRecords != null) && (!dataRecords.isEmpty())) {
            for (Record record : dataRecords) {
                LOG.debug("Processing record: " + record);
                String seqNum = record.getSequenceNumber();
                if (!processedSeqNums.contains(seqNum)) {
                    records.add(record);
                    processedSeqNums.add(seqNum);
                }
            }
        }
        if (dataRecords.isEmpty()) {
            numProcessRecordsCallsWithEmptyRecordList++;
        }
        try {
            checkpointer.checkpoint();
        } catch (ThrottlingException | ShutdownException
                | KinesisClientLibDependencyException | InvalidStateException e) {
            // Continue processing records and checkpoint next time if we get a transient error.
            // Don't checkpoint if the processor has been shutdown.
            LOG.debug("Caught exception while checkpointing: ", e);
        }

        if (sem != null) {
            sem.release(dataRecords.size());
        }
    }

    @Override
    public void shutdown(ShutdownInput input) {
        ShutdownReason reason = input.getShutdownReason();
        IRecordProcessorCheckpointer checkpointer = input.getCheckpointer();
        if (shardSequenceVerifier != null) {
            shardSequenceVerifier.registerShutdown(shardId, reason);
        }
        shutdownReason = reason;
        if (reason.equals(ShutdownReason.TERMINATE)) {
            try {
                checkpointer.checkpoint();
            } catch (KinesisClientLibNonRetryableException e) {
                LOG.error("Caught exception when checkpointing while shutdown.", e);
                throw new RuntimeException(e);
            }
        }

        shutdownLatch.countDown();
    }

    /**
     * @return the shardId
     */
    String getShardId() {
        return shardId;
    }

    /**
     * @return the shutdownReason
     */
    ShutdownReason getShutdownReason() {
        return shutdownReason;
    }

    /**
     * @return the numProcessRecordsCallsWithEmptyRecordList
     */
    long getNumProcessRecordsCallsWithEmptyRecordList() {
        return numProcessRecordsCallsWithEmptyRecordList;
    }

    boolean isShutdownNotificationCalled() {
        return shutdownNotificationCalled;
    }

    @Override
    public void shutdownRequested(IRecordProcessorCheckpointer checkpointer) {
        shutdownNotificationCalled = true;
        notifyShutdownLatch.countDown();
    }

    public CountDownLatch getInitializeLatch() {
        return initializeLatch;
    }

    public CountDownLatch getNotifyShutdownLatch() {
        return notifyShutdownLatch;
    }

    public CountDownLatch getShutdownLatch() {
        return shutdownLatch;
    }
}
