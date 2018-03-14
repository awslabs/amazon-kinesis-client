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

import software.amazon.kinesis.processor.v2.IRecordProcessorFactory;
import software.amazon.kinesis.processor.v2.IRecordProcessor;

/**
 * Adapts a V1 {@link software.amazon.kinesis.processor.IRecordProcessorFactory
 * IRecordProcessorFactory} to V2
 * {@link IRecordProcessorFactory IRecordProcessorFactory}.
 */
public class V1ToV2RecordProcessorFactoryAdapter implements IRecordProcessorFactory {

    private software.amazon.kinesis.processor.IRecordProcessorFactory factory;

    public V1ToV2RecordProcessorFactoryAdapter(
            software.amazon.kinesis.processor.IRecordProcessorFactory factory) {
        this.factory = factory;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new V1ToV2RecordProcessorAdapter(factory.createProcessor());
    }    
}
