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

package software.amazon.kinesis.retrieval.fanout;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.RetrievalFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private StreamIdentifier streamIdentifier;

    private FanOutConfig config;

    @Before
    public void setup() {
        config = spy(new FanOutConfig(kinesisClient))
                // DRY: set the most commonly-used parameters
                .applicationName(TEST_APPLICATION_NAME)
                .streamName(TEST_STREAM_NAME);
        doReturn(consumerRegistration)
                .when(config)
                .createConsumerRegistration(eq(kinesisClient), anyString(), anyString());
        when(streamConfig.streamIdentifier()).thenReturn(streamIdentifier);
        when(streamIdentifier.streamName()).thenReturn(TEST_STREAM_NAME);
    }

    @Test
    public void testNoRegisterIfConsumerArnSet() {
        config.consumerArn(TEST_CONSUMER_ARN)
                // unset common parameters
                .applicationName(null)
                .streamName(null);

        RetrievalFactory retrievalFactory = config.retrievalFactory();

        assertNotNull(retrievalFactory);
        verifyZeroInteractions(consumerRegistration);
    }

    @Test
    public void testRegisterCalledWhenConsumerArnUnset() throws Exception {
        getRecordsCache(null);

        verify(consumerRegistration).getOrCreateStreamConsumerArn();
    }

    @Test
    public void testRegisterNotCalledWhenConsumerArnSetInMultiStreamMode() throws Exception {
        when(streamConfig.consumerArn()).thenReturn("consumerArn");

        getRecordsCache("123456789012:stream:12345");

        verify(consumerRegistration, never()).getOrCreateStreamConsumerArn();
    }

    @Test
    public void testRegisterCalledWhenConsumerArnNotSetInMultiStreamMode() throws Exception {
        getRecordsCache("123456789012:stream:12345");

        verify(consumerRegistration).getOrCreateStreamConsumerArn();
    }

    @Test
    public void testDependencyExceptionInConsumerCreation() throws Exception {
        DependencyException de = new DependencyException("Bad", null);
        when(consumerRegistration.getOrCreateStreamConsumerArn()).thenThrow(de);

        try {
            getRecordsCache(null);
            Assert.fail("should throw");
        } catch (RuntimeException e) {
            verify(consumerRegistration).getOrCreateStreamConsumerArn();
            assertEquals(de, e.getCause());
        }
    }

    @Test
    public void testCreationWithApplicationName() {
        getRecordsCache(null);

        assertEquals(TEST_STREAM_NAME, config.streamName());
        assertEquals(TEST_APPLICATION_NAME, config.applicationName());
    }

    @Test
    public void testCreationWithConsumerName() {
        config.consumerName(TEST_CONSUMER_NAME)
                // unset common parameters
                .applicationName(null);

        getRecordsCache(null);

        assertEquals(TEST_STREAM_NAME, config.streamName());
        assertEquals(TEST_CONSUMER_NAME, config.consumerName());
    }

    @Test
    public void testCreationWithBothConsumerApplication() {
        config = config.consumerName(TEST_CONSUMER_NAME);

        getRecordsCache(null);

        assertEquals(TEST_STREAM_NAME, config.streamName());
        assertEquals(TEST_CONSUMER_NAME, config.consumerName());
    }

    @Test
    public void testValidState() {
        assertNull(config.consumerArn());
        assertNotNull(config.streamName());

        config.validateState(false);

        // both streamName and consumerArn are non-null
        config.consumerArn(TEST_CONSUMER_ARN);
        config.validateState(false);

        config.consumerArn(null);
        config.streamName(null);
        config.validateState(false);
        config.validateState(true);

        assertNull(config.streamName());
        assertNull(config.consumerArn());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStateMultiWithStreamName() {
        testInvalidState(TEST_STREAM_NAME, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStateMultiWithConsumerArn() {
        testInvalidState(null, TEST_CONSUMER_ARN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStateMultiWithStreamNameAndConsumerArn() {
        testInvalidState(TEST_STREAM_NAME, TEST_CONSUMER_ARN);
    }

    private void testInvalidState(final String streamName, final String consumerArn) {
        config.streamName(streamName);
        config.consumerArn(consumerArn);

        try {
            config.validateState(true);
        } finally {
            assertEquals(streamName, config.streamName());
            assertEquals(consumerArn, config.consumerArn());
        }
    }

    private void getRecordsCache(final String streamIdentifer) {
        final ShardInfo shardInfo = mock(ShardInfo.class);
        when(shardInfo.streamIdentifierSerOpt()).thenReturn(Optional.ofNullable(streamIdentifer));

        final RetrievalFactory factory = config.retrievalFactory();
        factory.createGetRecordsCache(shardInfo, streamConfig, mock(MetricsFactory.class));
    }
}
