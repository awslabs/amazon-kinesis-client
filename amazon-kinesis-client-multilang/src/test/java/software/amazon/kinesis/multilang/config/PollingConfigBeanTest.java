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

package software.amazon.kinesis.multilang.config;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class PollingConfigBeanTest {

    @Mock
    private KinesisAsyncClient kinesisAsyncClient;

    @Test
    public void testAllPropertiesTransit() {
        PollingConfigBean pollingConfigBean = new PollingConfigBean();
        pollingConfigBean.setIdleTimeBetweenReadsInMillis(1000);
        pollingConfigBean.setMaxGetRecordsThreadPool(20);
        pollingConfigBean.setMaxRecords(5000);
        pollingConfigBean.setRetryGetRecordsInSeconds(30);

        ConvertUtilsBean convertUtilsBean = new ConvertUtilsBean();
        BeanUtilsBean utilsBean = new BeanUtilsBean(convertUtilsBean);

        MultiLangDaemonConfiguration multiLangDaemonConfiguration = new MultiLangDaemonConfiguration(utilsBean, convertUtilsBean);
        multiLangDaemonConfiguration.setStreamName("test-stream");

        PollingConfig pollingConfig = pollingConfigBean.build(kinesisAsyncClient, multiLangDaemonConfiguration);

        assertThat(pollingConfig.kinesisClient(), equalTo(kinesisAsyncClient));
        assertThat(pollingConfig.streamName(), equalTo(multiLangDaemonConfiguration.getStreamName()));
        assertThat(pollingConfig.idleTimeBetweenReadsInMillis(), equalTo(pollingConfigBean.getIdleTimeBetweenReadsInMillis()));
        assertThat(pollingConfig.maxGetRecordsThreadPool(), equalTo(Optional.of(pollingConfigBean.getMaxGetRecordsThreadPool())));
        assertThat(pollingConfig.maxRecords(), equalTo(pollingConfigBean.getMaxRecords()));
        assertThat(pollingConfig.retryGetRecordsInSeconds(), equalTo(Optional.of(pollingConfigBean.getRetryGetRecordsInSeconds())));
    }

}