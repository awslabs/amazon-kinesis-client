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
package software.amazon.kinesis.processor;

import software.amazon.kinesis.lifecycle.InitializationInput;
import software.amazon.kinesis.lifecycle.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.ShutdownInput;
import software.amazon.kinesis.lifecycle.ShutdownReason;

/**
 * The Amazon Kinesis Client Library will instantiate record processors to process data records fetched from Amazon
 * Kinesis.
 */
public interface RecordProcessor {

    /**
     * Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
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
     * Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data records to this
     * RecordProcessor instance.
     *
     * <h2><b>Warning</b></h2>
     *
     * When the value of {@link ShutdownInput#shutdownReason()} is
     * {@link ShutdownReason#TERMINATE} it is required that you
     * checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
     *
     * @param shutdownInput
     *            Provides information and capabilities (eg checkpointing) related to shutdown of this record processor.
     */
    void shutdown(ShutdownInput shutdownInput);

}
