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
package com.amazonaws.services.kinesis.clientlibrary.interfaces.v2;

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

/**
 * The Amazon Kinesis Client Library will instantiate record processors to process data records fetched from Amazon
 * Kinesis.
 */
public interface IRecordProcessor {

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
     * When the value of {@link ShutdownInput#getShutdownReason()} is
     * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason#TERMINATE} it is required that you
     * checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
     *
     * @param shutdownInput
     *            Provides information and capabilities (eg checkpointing) related to shutdown of this record processor.
     */
    void shutdown(ShutdownInput shutdownInput);

}
