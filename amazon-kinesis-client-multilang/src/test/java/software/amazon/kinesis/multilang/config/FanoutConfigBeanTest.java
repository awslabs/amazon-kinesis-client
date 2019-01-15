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

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class FanoutConfigBeanTest {

    @Mock
    private KinesisAsyncClient kinesisAsyncClient;

    @Test
    public void testAllConfigurationTransits() {
        FanoutConfigBean fanoutConfigBean = new FanoutConfigBean();
        fanoutConfigBean.setConsumerArn("consumer-arn");
        fanoutConfigBean.setConsumerName("consumer-name");
        fanoutConfigBean.setMaxDescribeStreamConsumerRetries(10);
        fanoutConfigBean.setMaxDescribeStreamSummaryRetries(20);
        fanoutConfigBean.setRegisterStreamConsumerRetries(30);
        fanoutConfigBean.setRetryBackoffMillis(1000);

        ConvertUtilsBean convertUtilsBean = new ConvertUtilsBean();
        BeanUtilsBean utilsBean = new BeanUtilsBean(convertUtilsBean);

        MultiLangDaemonConfiguration configuration = new MultiLangDaemonConfiguration(utilsBean, convertUtilsBean);
        configuration.setStreamName("test-stream");
        configuration.setApplicationName("test-application");
        FanOutConfig fanOutConfig =fanoutConfigBean.build(kinesisAsyncClient, configuration);

        assertThat(fanOutConfig.kinesisClient(), equalTo(kinesisAsyncClient));
        assertThat(fanOutConfig.streamName(), equalTo(configuration.getStreamName()));
        assertThat(fanOutConfig.applicationName(), equalTo(configuration.getApplicationName()));
        assertThat(fanOutConfig.consumerArn(), equalTo(fanoutConfigBean.getConsumerArn()));
        assertThat(fanOutConfig.consumerName(), equalTo(fanoutConfigBean.getConsumerName()));
        assertThat(fanOutConfig.maxDescribeStreamConsumerRetries(), equalTo(fanoutConfigBean.getMaxDescribeStreamConsumerRetries()));
        assertThat(fanOutConfig.maxDescribeStreamSummaryRetries(), equalTo(fanoutConfigBean.getMaxDescribeStreamSummaryRetries()));
        assertThat(fanOutConfig.registerStreamConsumerRetries(), equalTo(fanoutConfigBean.getRegisterStreamConsumerRetries()));
        assertThat(fanOutConfig.retryBackoffMillis(), equalTo(fanoutConfigBean.getRetryBackoffMillis()));

    }

}