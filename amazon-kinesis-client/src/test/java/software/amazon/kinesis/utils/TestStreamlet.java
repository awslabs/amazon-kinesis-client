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
package software.amazon.kinesis.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.KinesisClientLibNonRetryableException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.leases.ShardSequenceVerifier;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShutdownNotificationAware;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * Streamlet that tracks records it's seen - useful for testing.
 */
@Slf4j
public class TestStreamlet implements ShardRecordProcessor, ShutdownNotificationAware {
    private List<KinesisClientRecord> records = new ArrayList<>();

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

    public TestStreamlet() {}

    public TestStreamlet(Semaphore sem, ShardSequenceVerifier shardSequenceVerifier) {
        this();
        this.sem = sem;
        this.shardSequenceVerifier = shardSequenceVerifier;
    }

    public List<KinesisClientRecord> getProcessedRecords() {
        return records;
    }

    @Override
    public void initialize(InitializationInput input) {
        shardId = input.shardId();
        if (shardSequenceVerifier != null) {
            shardSequenceVerifier.registerInitialization(shardId);
        }
        initializeLatch.countDown();
    }

    @Override
    public void processRecords(ProcessRecordsInput input) {
        List<KinesisClientRecord> dataRecords = input.records();
        RecordProcessorCheckpointer checkpointer = input.checkpointer();
        if ((dataRecords != null) && (!dataRecords.isEmpty())) {
            for (KinesisClientRecord record : dataRecords) {
                log.debug("Processing record: {}", record);
                String seqNum = record.sequenceNumber();
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
        } catch (ThrottlingException
                | ShutdownException
                | KinesisClientLibDependencyException
                | InvalidStateException e) {
            // Continue processing records and checkpoint next time if we get a transient error.
            // Don't checkpoint if the processor has been shutdown.
            log.debug("Caught exception while checkpointing: ", e);
        }

        if (sem != null) {
            sem.release(dataRecords.size());
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        if (shardSequenceVerifier != null) {
            shardSequenceVerifier.registerShutdown(shardId, ShutdownReason.LEASE_LOST);
        }
        shutdownLatch.countDown();
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        if (shardSequenceVerifier != null) {
            shardSequenceVerifier.registerShutdown(shardId, ShutdownReason.SHARD_END);
        }
        try {
            shardEndedInput.checkpointer().checkpoint();
        } catch (KinesisClientLibNonRetryableException e) {
            log.error("Caught exception when checkpointing while shutdown.", e);
            throw new RuntimeException(e);
        }
        shutdownLatch.countDown();
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {}

    /**
     * @return the shardId
     */
    public String getShardId() {
        return shardId;
    }

    /**
     * @return the shutdownReason
     */
    public ShutdownReason getShutdownReason() {
        return shutdownReason;
    }

    /**
     * @return the numProcessRecordsCallsWithEmptyRecordList
     */
    public long getNumProcessRecordsCallsWithEmptyRecordList() {
        return numProcessRecordsCallsWithEmptyRecordList;
    }

    public boolean isShutdownNotificationCalled() {
        return shutdownNotificationCalled;
    }

    @Override
    public void shutdownRequested(RecordProcessorCheckpointer checkpointer) {
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
