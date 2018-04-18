/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.multilang;

import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import org.junit.Assert;
import org.junit.Test;

import software.amazon.kinesis.processor.RecordProcessor;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamingRecordProcessorFactoryTest {

    @Mock
    private KinesisClientLibConfiguration configuration;

    @Test
    public void createProcessorTest() {
        MultiLangRecordProcessorFactory factory = new MultiLangRecordProcessorFactory("somecommand", null, configuration);
        RecordProcessor processor = factory.createProcessor();

        Assert.assertEquals("Should have constructed a StreamingRecordProcessor", MultiLangRecordProcessor.class,
                processor.getClass());
    }
}
