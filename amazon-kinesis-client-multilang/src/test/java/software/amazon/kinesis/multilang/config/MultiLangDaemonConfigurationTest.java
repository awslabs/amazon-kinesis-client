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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

@RunWith(MockitoJUnitRunner.class)
public class MultiLangDaemonConfigurationTest {

    private static final String AWS_REGION_PROPERTY_NAME = "aws.region";

    private BeanUtilsBean utilsBean;
    private ConvertUtilsBean convertUtilsBean;
    private String originalRegionValue;

    @Mock
    private ShardRecordProcessorFactory shardRecordProcessorFactory;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        originalRegionValue = System.getProperty(AWS_REGION_PROPERTY_NAME);
        System.setProperty(AWS_REGION_PROPERTY_NAME, "us-east-1");
        convertUtilsBean = new ConvertUtilsBean();
        utilsBean = new BeanUtilsBean(convertUtilsBean);
    }

    @After
    public void after() {
        if (originalRegionValue != null) {
            System.setProperty(AWS_REGION_PROPERTY_NAME, originalRegionValue);
        } else {
            System.clearProperty(AWS_REGION_PROPERTY_NAME);
        }
    }


    public MultiLangDaemonConfiguration baseConfiguration() {
        MultiLangDaemonConfiguration configuration = new MultiLangDaemonConfiguration(utilsBean, convertUtilsBean);
        configuration.setApplicationName("Test");
        configuration.setStreamName("Test");
        configuration.getKinesisCredentialsProvider().set("class", DefaultCredentialsProvider.class.getName());

        return configuration;
    }

    @Test
    public void testSetPrimitiveValue() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setMaxLeasesForWorker(10);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration
                .resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.leaseManagementConfig.maxLeasesForWorker(), equalTo(10));
    }

    @Test
    public void testSetEnablePriorityLeaseAssignment() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setEnablePriorityLeaseAssignment(false);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration.resolvedConfiguration(
                shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.leaseManagementConfig.enablePriorityLeaseAssignment(), equalTo(false));
    }

    @Test
    public void testDefaultRetrievalConfig() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration
                .resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(),
                instanceOf(FanOutConfig.class));
    }

    @Test
    public void testDefaultRetrievalConfigWithPollingConfigSet() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setMaxRecords(10);
        configuration.setIdleTimeBetweenReadsInMillis(60000);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration
                .resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(),
                instanceOf(PollingConfig.class));
        assertEquals(10,
            ((PollingConfig) resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig()).maxRecords());
        assertEquals(60000,
            ((PollingConfig) resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig())
                .idleTimeBetweenReadsInMillis());
        assertTrue(((PollingConfig) resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig())
            .usePollingConfigIdleTimeValue());
    }

    @Test
    public void testFanoutRetrievalMode() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setRetrievalMode(RetrievalMode.FANOUT);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration
                .resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(),
                instanceOf(FanOutConfig.class));
    }

    @Test
    public void testPollingRetrievalMode() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setRetrievalMode(RetrievalMode.POLLING);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration
                .resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(),
                instanceOf(PollingConfig.class));
    }

    @Test
    public void testRetrievalModeSetForPollingString() throws Exception {
        MultiLangDaemonConfiguration configuration = baseConfiguration();

        utilsBean.setProperty(configuration, "retrievalMode", RetrievalMode.POLLING.name().toLowerCase());

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration
                .resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(),
                instanceOf(PollingConfig.class));
    }

    @Test
    public void testRetrievalModeSetForFanoutString() throws Exception {
        MultiLangDaemonConfiguration configuration = baseConfiguration();

        utilsBean.setProperty(configuration, "retrievalMode", RetrievalMode.FANOUT.name().toLowerCase());

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration
                .resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(),
                instanceOf(FanOutConfig.class));
    }

    @Test
    public void testInvalidRetrievalMode() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unknown retrieval type");

        MultiLangDaemonConfiguration configuration = baseConfiguration();

        utilsBean.setProperty(configuration, "retrievalMode", "invalid");
    }

    // @Test
    // TODO : Enable this test once https://github.com/awslabs/amazon-kinesis-client/issues/692 is resolved
    public void testmetricsEnabledDimensions() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setMetricsEnabledDimensions(new String[]{"Operation"});
        configuration.resolvedConfiguration(shardRecordProcessorFactory);
    }

    @Test
    public void testFanoutConfigSetConsumerName() {
        String consumerArn = "test-consumer";

        MultiLangDaemonConfiguration configuration = baseConfiguration();

        configuration.setRetrievalMode(RetrievalMode.FANOUT);
        configuration.getFanoutConfig().setConsumerArn(consumerArn);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration = configuration
                .resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(),
                instanceOf(FanOutConfig.class));
        FanOutConfig fanOutConfig = (FanOutConfig) resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig();

        assertThat(fanOutConfig.consumerArn(), equalTo(consumerArn));
    }

}