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

import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;

/**
 * The Amazon Kinesis Client Library will instantiate record processors to process data records fetched from Amazon
 * Kinesis.
 */
public interface ShardRecordProcessor {

    /**
     * Invoked by the Amazon Kinesis Client Library before data records are delivered to the ShardRecordProcessor instance
     * (via processRecords).
     *
     * @param initializationInput Provides information related to initialization 
     */
    void initialize(InitializationInput initializationInput);

    /**
     * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
     * application.
     * Upon fail over, the new instance will get records with sequence number > checkpoint position
     * for each partition key.
     *
     * @param processRecordsInput Provides the records to be processed as well as information and capabilities related
     *        to them (eg checkpointing).
     */
    void processRecords(ProcessRecordsInput processRecordsInput);

    /**
     * Called when the lease that tied to this record processor has been lost. Once the lease has been lost the record
     * processor can no longer checkpoint.
     * 
     * @param leaseLostInput
     *            access to functions and data related to the loss of the lease. Currently this has no functionality.
     */
    void leaseLost(LeaseLostInput leaseLostInput);

    /**
     * Called when the shard that this record process is handling has been completed. Once a shard has been completed no
     * further records will ever arrive on that shard.
     *
     * When this is called the record processor <b>must</b> call {@link RecordProcessorCheckpointer#checkpoint()},
     * otherwise an exception will be thrown and the all child shards of this shard will not make progress.
     * 
     * @param shardEndedInput
     *            provides access to a checkpointer method for completing processing of the shard.
     */
    void shardEnded(ShardEndedInput shardEndedInput);

    /**
     * Called when the Scheduler has been requested to shutdown. This is called while the record processor still holds
     * the lease so checkpointing is possible. Once this method has completed the lease for the record processor is
     * released, and {@link #leaseLost(LeaseLostInput)} will be called at a later time.
     *
     * @param shutdownRequestedInput
     *            provides access to a checkpointer allowing a record processor to checkpoint before the shutdown is
     *            completed.
     */
    void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput);

}
