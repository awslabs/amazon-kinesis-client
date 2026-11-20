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
package software.amazon.kinesis.lifecycle;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Task for initializing shard position and invoking the ShardRecordProcessor initialize() API.
 */
@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class InitializeTask implements ConsumerTask {
    private static final String INITIALIZE_TASK_OPERATION = "InitializeTask";
    private static final String RECORD_PROCESSOR_INITIALIZE_METRIC = "RecordProcessor.initialize";

    @NonNull
    private final ShardInfo shardInfo;

    @NonNull
    private final ShardRecordProcessor shardRecordProcessor;

    @NonNull
    private final LeaseCoordinator leaseCoordinator;

    @NonNull
    private final ShardRecordProcessorCheckpointer recordProcessorCheckpointer;

    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;

    @NonNull
    private final RecordsPublisher cache;

    // Back off for this interval if we encounter a problem (exception)
    private final long backoffTimeMillis;

    @NonNull
    private final MetricsFactory metricsFactory;

    private final TaskType taskType = TaskType.INITIALIZE;

    /*
     * Initializes the data fetcher (position in shard) and invokes the ShardRecordProcessor initialize() API.
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#call()
     */
    @Override
    public TaskResult call() {
        boolean applicationException = false;
        Exception exception = null;

        try {
            log.debug("Initializing ShardId {}", shardInfo);
            final String leaseKey = ShardInfo.getLeaseKey(shardInfo);
            Checkpoint initialCheckpointObject = getInitialCheckpointObject(leaseKey);
            ExtendedSequenceNumber initialCheckpoint = initialCheckpointObject.checkpoint();
            log.debug(
                    "[{}]: Checkpoint: {} -- Initial Position: {}",
                    leaseKey,
                    initialCheckpoint,
                    initialPositionInStream);

            cache.start(initialCheckpoint, initialPositionInStream);

            recordProcessorCheckpointer.largestPermittedCheckpointValue(initialCheckpoint);
            recordProcessorCheckpointer.setInitialCheckpointValue(initialCheckpoint);

            log.debug("Calling the record processor initialize().");
            final InitializationInput initializationInput = InitializationInput.builder()
                    .shardId(shardInfo.shardId())
                    .extendedSequenceNumber(initialCheckpoint)
                    .pendingCheckpointSequenceNumber(initialCheckpointObject.pendingCheckpoint())
                    .pendingCheckpointState(initialCheckpointObject.pendingCheckpointState())
                    .build();

            final MetricsScope scope =
                    MetricsUtil.createMetricsWithOperation(metricsFactory, INITIALIZE_TASK_OPERATION);

            final long startTime = System.currentTimeMillis();
            try {
                shardRecordProcessor.initialize(initializationInput);
                log.debug("Record processor initialize() completed.");
            } catch (Exception e) {
                applicationException = true;
                throw e;
            } finally {
                MetricsUtil.addLatency(scope, RECORD_PROCESSOR_INITIALIZE_METRIC, startTime, MetricsLevel.SUMMARY);
                MetricsUtil.endScope(scope);
            }

            return new TaskResult(null);
        } catch (Exception e) {
            if (applicationException) {
                log.error("Application initialize() threw exception: ", e);
            } else {
                log.error("Caught exception: ", e);
            }
            exception = e;
            // backoff if we encounter an exception.
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                log.debug("Interrupted sleep", ie);
            }
        }

        return new TaskResult(exception);
    }

    /**
     * Uses the checkpoint from the locally held lease instead of reading it from DynamoDB. The remote read is
     * eventually consistent and can return a stale checkpoint, while lease renewal keeps going on the local copy's
     * leaseCounter, so the consumer would process from an old checkpoint without detecting it. It also costs a
     * DynamoDB read on every initialize attempt, including each retry after a failure.
     */
    private Checkpoint getInitialCheckpointObject(final String leaseKey) throws KinesisClientLibException {
        final Lease lease = leaseCoordinator.getCurrentlyHeldLease(leaseKey);
        if (lease != null) {
            log.debug("[{}] Using checkpoint from currently held lease => {}", leaseKey, lease);
            return new Checkpoint(lease.checkpoint(), lease.pendingCheckpoint(), lease.pendingCheckpointState());
        }
        // The lease was lost; the ShardConsumer shuts this consumer down.
        throw new ShutdownException("Lease is not currently held, shutting down " + leaseKey);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#taskType()
     */
    @Override
    public TaskType taskType() {
        return taskType;
    }
}
