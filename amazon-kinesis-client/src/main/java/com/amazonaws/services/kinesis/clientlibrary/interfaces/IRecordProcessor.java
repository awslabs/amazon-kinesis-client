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
package com.amazonaws.services.kinesis.clientlibrary.interfaces;

import java.util.List;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;

/**
 * The Amazon Kinesis Client Library will instantiate record processors to process data records fetched from Amazon
 * Kinesis.
 */
public interface IRecordProcessor {

    /**
     * Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
     * (via processRecords).
     * 
     * @param shardId The record processor will be responsible for processing records of this shard.
     */
    void initialize(String shardId);

    /**
     * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
     * application.
     * Upon fail over, the new instance will get records with sequence number > checkpoint position
     * for each partition key.
     * 
     * @param records Data records to be processed
     * @param checkpointer RecordProcessor should use this instance to checkpoint their progress.
     */
    void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer);

    /**
     * Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data records to this
     * RecordProcessor instance. The reason parameter indicates:
     * a/ ShutdownReason.TERMINATE - The shard has been closed and there will not be any more records to process. The
     * record processor should checkpoint (after doing any housekeeping) to acknowledge that it has successfully
     * completed processing all records in this shard.
     * b/ ShutdownReason.ZOMBIE: A fail over has occurred and a different record processor is (or will be) responsible
     * for processing records.
     * 
     * @param checkpointer RecordProcessor should use this instance to checkpoint.
     * @param reason Reason for the shutdown (ShutdownReason.TERMINATE indicates the shard is closed and there are no
     *        more records to process. Shutdown.ZOMBIE indicates a fail over has occurred).
     */
    void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason);

}
