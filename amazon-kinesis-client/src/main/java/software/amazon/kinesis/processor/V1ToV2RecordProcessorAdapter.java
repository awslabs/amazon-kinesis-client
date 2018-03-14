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

import software.amazon.kinesis.processor.v2.IRecordProcessor;
import software.amazon.kinesis.lifecycle.InitializationInput;
import software.amazon.kinesis.lifecycle.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.ShutdownInput;

/**
 * Adapts a V1 {@link software.amazon.kinesis.processor.IRecordProcessor IRecordProcessor}
 * to V2 {@link IRecordProcessor IRecordProcessor}.
 */
class V1ToV2RecordProcessorAdapter implements IRecordProcessor {

    private software.amazon.kinesis.processor.IRecordProcessor recordProcessor;
    
    V1ToV2RecordProcessorAdapter(
            software.amazon.kinesis.processor.IRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        recordProcessor.initialize(initializationInput.getShardId());  
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        recordProcessor.processRecords(processRecordsInput.getRecords(), processRecordsInput.getCheckpointer());
        
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        recordProcessor.shutdown(shutdownInput.getCheckpointer(), shutdownInput.getShutdownReason());
    }

}
