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
package software.amazon.kinesis.processor;

import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;

/**
 * Used by RecordProcessors when they want to checkpoint their progress.
 * The Amazon Kinesis Client Library will pass an object implementing this interface to RecordProcessors, so they can
 * checkpoint their progress.
 */
public interface RecordProcessorCheckpointer {

    /**
     * This method will checkpoint the progress at the last data record that was delivered to the record processor.
     * Upon fail over (after a successful checkpoint() call), the new/replacement ShardRecordProcessor instance
     * will receive data records whose sequenceNumber > checkpoint position (for each partition key).
     * In steady state, applications should checkpoint periodically (e.g. once every 5 minutes).
     * Calling this API too frequently can slow down the application (because it puts pressure on the underlying
     * checkpoint storage layer).
     *
     * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
     *         backoff and retry.
     */
    void checkpoint()
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException;

    /**
     * This method will checkpoint the progress at the provided record. This method is analogous to
     * {@link #checkpoint()} but provides the ability to specify the record at which to
     * checkpoint.
     *
     * @param record A record at which to checkpoint in this shard. Upon failover,
     *        the Kinesis Client Library will start fetching records after this record's sequence number.
     * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
     *         backoff and retry.
     */
    void checkpoint(Record record)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException;

    /**
     * This method will checkpoint the progress at the provided sequenceNumber. This method is analogous to
     * {@link #checkpoint()} but provides the ability to specify the sequence number at which to
     * checkpoint.
     *
     * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover,
     *        the Kinesis Client Library will start fetching records after this sequence number.
     * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
     *         backoff and retry.
     * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
     *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
     *         greatest sequence number seen by the associated record processor.
     *         2.) It is not a valid sequence number for a record in this shard.
     */
    void checkpoint(String sequenceNumber)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
                    IllegalArgumentException;

    /**
     * This method will checkpoint the progress at the provided sequenceNumber and subSequenceNumber, the latter for
     * aggregated records produced with the Producer Library. This method is analogous to {@link #checkpoint()}
     * but provides the ability to specify the sequence and subsequence numbers at which to checkpoint.
     *
     * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover, the Kinesis
     *        Client Library will start fetching records after the given sequence and subsequence numbers.
     * @param subSequenceNumber A subsequence number at which to checkpoint within this shard. Upon failover, the
     *        Kinesis Client Library will start fetching records after the given sequence and subsequence numbers.
     * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
     *         backoff and retry.
     * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
     *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
     *         greatest sequence number seen by the associated record processor.
     *         2.) It is not a valid sequence number for a record in this shard.
     */
    void checkpoint(String sequenceNumber, long subSequenceNumber)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
                    IllegalArgumentException;

    /**
     * This method will record a pending checkpoint at the last data record that was delivered to the record processor.
     * If the application fails over between calling prepareCheckpoint() and checkpoint(), the init() method of the next
     * IRecordProcessor for this shard will be informed of the prepared sequence number
     *
     * Application should use this to assist with idempotency across failover by calling prepareCheckpoint before having
     * side effects, then by calling checkpoint on the returned PreparedCheckpointer after side effects are complete.
     * Use the sequence number passed in to init() to behave idempotently.
     *
     * @return an PreparedCheckpointer object that can be called later to persist the checkpoint.
     *
     * @throws ThrottlingException Can't store pending checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store pending checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the pending checkpoint. The
     *         application can backoff and retry.
     */
    PreparedCheckpointer prepareCheckpoint()
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException;

    /**
     * This method will record a pending checkpoint at the last data record that was delivered to the record processor.
     * If the application fails over between calling prepareCheckpoint() and checkpoint(), the init() method of the next
     * IRecordProcessor for this shard will be informed of the prepared sequence number and application state.
     *
     * Application should use this to assist with idempotency across failover by calling prepareCheckpoint before having
     * side effects, then by calling checkpoint on the returned PreparedCheckpointer after side effects are complete.
     * Use the sequence number and application state passed in to init() to behave idempotently.
     *
     * @param applicationState arbitrary application state that will be passed to the next record processor that
     *        processes the shard.
     * @return an PreparedCheckpointer object that can be called later to persist the checkpoint.
     *
     * @throws ThrottlingException Can't store pending checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store pending checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the pending checkpoint. The
     *         application can backoff and retry.
     */
    PreparedCheckpointer prepareCheckpoint(byte[] applicationState)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException;

    /**
     * This method will record a pending checkpoint at the at the provided record. This method is analogous to
     * {@link #prepareCheckpoint()} but provides the ability to specify the record at which to prepare the checkpoint.
     *
     * @param record A record at which to prepare checkpoint in this shard.
     *
     * Application should use this to assist with idempotency across failover by calling prepareCheckpoint before having
     * side effects, then by calling checkpoint on the returned PreparedCheckpointer after side effects are complete.
     * Use the sequence number and application state passed in to init() to behave idempotently.
     *
     * @return an PreparedCheckpointer object that can be called later to persist the checkpoint.
     *
     * @throws ThrottlingException Can't store pending checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store pending checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the pending checkpoint. The
     *         application can backoff and retry.
     * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
     *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
     *         greatest sequence number seen by the associated record processor.
     *         2.) It is not a valid sequence number for a record in this shard.
     */
    PreparedCheckpointer prepareCheckpoint(Record record)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException;

    /**
     * This method will record a pending checkpoint at the at the provided record. This method is analogous to
     * {@link #prepareCheckpoint()} but provides the ability to specify the record and application state at which to
     * prepare the checkpoint.
     *
     * @param record A record at which to prepare checkpoint in this shard.
     * @param applicationState arbitrary application state that will be passed to the next record processor that
     *        processes the shard.
     *
     * Application should use this to assist with idempotency across failover by calling prepareCheckpoint before having
     * side effects, then by calling checkpoint on the returned PreparedCheckpointer after side effects are complete.
     * Use the sequence number and application state passed in to init() to behave idempotently.
     *
     * @return an PreparedCheckpointer object that can be called later to persist the checkpoint.
     *
     * @throws ThrottlingException Can't store pending checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store pending checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the pending checkpoint. The
     *         application can backoff and retry.
     * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
     *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
     *         greatest sequence number seen by the associated record processor.
     *         2.) It is not a valid sequence number for a record in this shard.
     */
    PreparedCheckpointer prepareCheckpoint(Record record, byte[] applicationState)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException;

    /**
     * This method will record a pending checkpoint at the provided sequenceNumber. This method is analogous to
     * {@link #prepareCheckpoint()} but provides the ability to specify the sequence number at which to checkpoint.
     *
     * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.
     *
     * @return an PreparedCheckpointer object that can be called later to persist the checkpoint.
     *
     * @throws ThrottlingException Can't store pending checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store pending checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the pending checkpoint. The
     *         application can backoff and retry.
     * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
     *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
     *         greatest sequence number seen by the associated record processor.
     *         2.) It is not a valid sequence number for a record in this shard.
     */
    PreparedCheckpointer prepareCheckpoint(String sequenceNumber)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
                    IllegalArgumentException;

    /**
     * This method will record a pending checkpoint at the provided sequenceNumber. This method is analogous to
     * {@link #prepareCheckpoint()} but provides the ability to specify the sequence number and application state
     * at which to checkpoint.
     *
     * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.
     * @param applicationState arbitrary application state that will be passed to the next record processor that
     *        processes the shard.
     *
     * @return an PreparedCheckpointer object that can be called later to persist the checkpoint.
     *
     * @throws ThrottlingException Can't store pending checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store pending checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the pending checkpoint. The
     *         application can backoff and retry.
     * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
     *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
     *         greatest sequence number seen by the associated record processor.
     *         2.) It is not a valid sequence number for a record in this shard.
     */
    PreparedCheckpointer prepareCheckpoint(String sequenceNumber, byte[] applicationState)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
                    IllegalArgumentException;

    /**
     * This method will record a pending checkpoint at the provided sequenceNumber and subSequenceNumber, the latter for
     * aggregated records produced with the Producer Library. This method is analogous to  {@link #prepareCheckpoint()}
     * but provides the ability to specify the sequence number at which to checkpoint
     *
     * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.
     * @param subSequenceNumber A subsequence number at which to prepare checkpoint within this shard.
     *
     * @return an PreparedCheckpointer object that can be called later to persist the checkpoint.
     *
     * @throws ThrottlingException Can't store pending checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store pending checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the pending checkpoint. The
     *         application can backoff and retry.
     * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
     *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
     *         greatest sequence number seen by the associated record processor.
     *         2.) It is not a valid sequence number for a record in this shard.
     */
    PreparedCheckpointer prepareCheckpoint(String sequenceNumber, long subSequenceNumber)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
                    IllegalArgumentException;

    /**
     * This method will record a pending checkpoint at the provided sequenceNumber and subSequenceNumber, the latter for
     * aggregated records produced with the Producer Library. This method is analogous to {@link #prepareCheckpoint()}
     * but provides the ability to specify the sequence number, subsequence number, and application state at which to
     * checkpoint.
     *
     * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.
     * @param subSequenceNumber A subsequence number at which to prepare checkpoint within this shard.
     * @param applicationState arbitrary application state that will be passed to the next record processor that
     *        processes the shard.
     *
     * @return an PreparedCheckpointer object that can be called later to persist the checkpoint.
     *
     * @throws ThrottlingException Can't store pending checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this ShardRecordProcessor instance.
     * @throws InvalidStateException Can't store pending checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the pending checkpoint. The
     *         application can backoff and retry.
     * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
     *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
     *         greatest sequence number seen by the associated record processor.
     *         2.) It is not a valid sequence number for a record in this shard.
     */
    PreparedCheckpointer prepareCheckpoint(String sequenceNumber, long subSequenceNumber, byte[] applicationState)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
                    IllegalArgumentException;

    Checkpointer checkpointer();
}
