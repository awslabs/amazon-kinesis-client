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

import software.amazon.aws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import software.amazon.aws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;

/**
 * Adapts a V1 {@link software.amazon.aws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
 * IRecordProcessorFactory} to V2
 * {@link IRecordProcessorFactory IRecordProcessorFactory}.
 */
class V1ToV2RecordProcessorFactoryAdapter implements IRecordProcessorFactory {

    private software.amazon.aws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory factory;

    V1ToV2RecordProcessorFactoryAdapter(
            software.amazon.aws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory factory) {
        this.factory = factory;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new V1ToV2RecordProcessorAdapter(factory.createProcessor());
    }    
}
