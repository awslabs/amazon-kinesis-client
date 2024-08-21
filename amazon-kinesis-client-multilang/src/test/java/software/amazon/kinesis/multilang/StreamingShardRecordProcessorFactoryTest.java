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
package software.amazon.kinesis.multilang;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.multilang.config.MultiLangDaemonConfiguration;
import software.amazon.kinesis.processor.ShardRecordProcessor;

@RunWith(MockitoJUnitRunner.class)
public class StreamingShardRecordProcessorFactoryTest {

    @Mock
    private MultiLangDaemonConfiguration configuration;

    @Test
    public void createProcessorTest() {
        MultiLangRecordProcessorFactory factory =
                new MultiLangRecordProcessorFactory("somecommand", null, configuration);
        ShardRecordProcessor processor = factory.shardRecordProcessor();

        Assert.assertEquals(
                "Should have constructed a StreamingRecordProcessor",
                MultiLangShardRecordProcessor.class,
                processor.getClass());
    }
}
