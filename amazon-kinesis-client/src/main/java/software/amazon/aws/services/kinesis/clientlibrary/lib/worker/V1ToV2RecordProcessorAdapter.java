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
package software.amazon.aws.services.kinesis.clientlibrary.lib.worker;

import software.amazon.aws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import software.amazon.aws.services.kinesis.clientlibrary.types.InitializationInput;
import software.amazon.aws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import software.amazon.aws.services.kinesis.clientlibrary.types.ShutdownInput;

/**
 * Adapts a V1 {@link software.amazon.aws.services.kinesis.clientlibrary.interfaces.IRecordProcessor IRecordProcessor}
 * to V2 {@link IRecordProcessor IRecordProcessor}.
 */
class V1ToV2RecordProcessorAdapter implements IRecordProcessor {

    private software.amazon.aws.services.kinesis.clientlibrary.interfaces.IRecordProcessor recordProcessor;
    
    V1ToV2RecordProcessorAdapter(
            software.amazon.aws.services.kinesis.clientlibrary.interfaces.IRecordProcessor recordProcessor) {
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
