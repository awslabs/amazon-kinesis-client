/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.kinesis.multilang.config;

import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;

@Getter
@Setter
public class FanoutConfigBean implements RetrievalConfigBuilder {

    @ConfigurationSettable(configurationClass = FanOutConfig.class)
    private int maxDescribeStreamSummaryRetries;
    @ConfigurationSettable(configurationClass = FanOutConfig.class)
    private String consumerArn;
    @ConfigurationSettable(configurationClass = FanOutConfig.class)
    private String consumerName;
    @ConfigurationSettable(configurationClass = FanOutConfig.class)
    private int maxDescribeStreamConsumerRetries;
    @ConfigurationSettable(configurationClass = FanOutConfig.class)
    private int registerStreamConsumerRetries;
    @ConfigurationSettable(configurationClass = FanOutConfig.class)
    private long retryBackoffMillis;

    @Override
    public FanOutConfig build(KinesisAsyncClient kinesisAsyncClient, MultiLangDaemonConfiguration parent) {
        return ConfigurationSettableUtils.resolveFields(this, new FanOutConfig(kinesisAsyncClient).applicationName(parent.getApplicationName())
                .streamName(parent.getStreamName()));
    }

}
