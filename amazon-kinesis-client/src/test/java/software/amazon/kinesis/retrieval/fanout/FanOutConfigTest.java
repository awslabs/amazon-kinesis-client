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

package software.amazon.kinesis.retrieval.fanout;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.retrieval.RetrievalFactory;

@RunWith(MockitoJUnitRunner.class)
public class FanOutConfigTest {

    private static final String TEST_CONSUMER_ARN = "TestConsumerArn";
    private static final String TEST_APPLICATION_NAME = "TestApplication";
    private static final String TEST_STREAM_NAME = "TestStream";
    private static final String TEST_CONSUMER_NAME = "TestConsumerName";

    @Mock
    private FanOutConsumerRegistration consumerRegistration;
    @Mock
    private KinesisAsyncClient kinesisClient;

    @Test
    public void testNoRegisterIfConsumerArnSet() throws Exception {
        FanOutConfig config = new TestingConfig(kinesisClient).consumerArn(TEST_CONSUMER_ARN);
        RetrievalFactory retrievalFactory = config.retrievalFactory();

        assertThat(retrievalFactory, not(nullValue()));
        verify(consumerRegistration, never()).getOrCreateStreamConsumerArn();
    }

    @Test
    public void testRegisterCalledWhenConsumerArnUnset() throws Exception {
        FanOutConfig config = new TestingConfig(kinesisClient).applicationName(TEST_APPLICATION_NAME)
                .streamName(TEST_STREAM_NAME);
        RetrievalFactory retrievalFactory = config.retrievalFactory();

        assertThat(retrievalFactory, not(nullValue()));
        verify(consumerRegistration).getOrCreateStreamConsumerArn();
    }

    @Test
    public void testDependencyExceptionInConsumerCreation() throws Exception {
        FanOutConfig config = new TestingConfig(kinesisClient).applicationName(TEST_APPLICATION_NAME)
                .streamName(TEST_STREAM_NAME);
        DependencyException de = new DependencyException("Bad", null);
        when(consumerRegistration.getOrCreateStreamConsumerArn()).thenThrow(de);
        try {
            config.retrievalFactory();
        } catch (RuntimeException e) {
            verify(consumerRegistration).getOrCreateStreamConsumerArn();
            assertThat(e.getCause(), equalTo(de));
        }
    }

    @Test
    public void testCreationWithApplicationName() throws Exception {
        FanOutConfig config = new TestingConfig(kinesisClient).applicationName(TEST_APPLICATION_NAME)
                .streamName(TEST_STREAM_NAME);
        RetrievalFactory factory = config.retrievalFactory();

        assertThat(factory, not(nullValue()));

        TestingConfig testingConfig = (TestingConfig) config;
        assertThat(testingConfig.stream, equalTo(TEST_STREAM_NAME));
        assertThat(testingConfig.consumerToCreate, equalTo(TEST_APPLICATION_NAME));
    }

    @Test
    public void testCreationWithConsumerName() throws Exception {
        FanOutConfig config = new TestingConfig(kinesisClient).consumerName(TEST_CONSUMER_NAME)
                .streamName(TEST_STREAM_NAME);
        RetrievalFactory factory = config.retrievalFactory();

        assertThat(factory, not(nullValue()));

        TestingConfig testingConfig = (TestingConfig) config;
        assertThat(testingConfig.stream, equalTo(TEST_STREAM_NAME));
        assertThat(testingConfig.consumerToCreate, equalTo(TEST_CONSUMER_NAME));
    }

    @Test
    public void testCreationWithBothConsumerApplication() throws Exception {
        FanOutConfig config = new TestingConfig(kinesisClient).applicationName(TEST_APPLICATION_NAME)
                .consumerName(TEST_CONSUMER_NAME).streamName(TEST_STREAM_NAME);
        RetrievalFactory factory = config.retrievalFactory();

        assertThat(factory, not(nullValue()));

        TestingConfig testingConfig = (TestingConfig) config;
        assertThat(testingConfig.stream, equalTo(TEST_STREAM_NAME));
        assertThat(testingConfig.consumerToCreate, equalTo(TEST_CONSUMER_NAME));
    }

    private class TestingConfig extends FanOutConfig {

        String stream;
        String consumerToCreate;

        public TestingConfig(KinesisAsyncClient kinesisClient) {
            super(kinesisClient);
        }

        @Override
        protected FanOutConsumerRegistration createConsumerRegistration(KinesisAsyncClient client, String stream,
                                                                        String consumerToCreate) {
            this.stream = stream;
            this.consumerToCreate = consumerToCreate;
            return consumerRegistration;
        }
    }

}