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

import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.awssdk.services.kinesis.model.ConsumerDescription;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.leases.exceptions.DependencyException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class FanOutConsumerRegistrationTest {
    private static final String STREAM_NAME = "TestStream";
    private static final String CONSUMER_NAME = "TestConsumer";
    private static final String STREAM_ARN = "TestStreamArn";
    private static final String CONSUMER_ARN = "TestConsumerArn";
    private static final int MAX_DSS_RETRIES = 5;
    private static final int MAX_DSC_RETRIES = 5;
    private static final int RSC_RETRIES = 5;
    private static final long BACKOFF_MILLIS = 50L;

    @Mock
    private KinesisAsyncClient client;

    private FanOutConsumerRegistration consumerRegistration;

    @Before
    public void setup() {
        consumerRegistration = new FanOutConsumerRegistration(
                client, STREAM_NAME, CONSUMER_NAME, MAX_DSS_RETRIES, MAX_DSC_RETRIES, RSC_RETRIES, BACKOFF_MILLIS);
    }

    @Test
    public void testConsumerAlreadyExists() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture =
                CompletableFuture.completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture =
                CompletableFuture.completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.ACTIVE));

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class)))
                .thenReturn(dscFuture);

        final String consumerArn = consumerRegistration.getOrCreateStreamConsumerArn();

        assertThat(consumerArn, equalTo(CONSUMER_ARN));

        verify(client, never()).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
    }

    @Test
    public void testConsumerAlreadyExistsMultipleCalls() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture =
                CompletableFuture.completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture =
                CompletableFuture.completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.ACTIVE));

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class)))
                .thenReturn(dscFuture);

        final String firstCall = consumerRegistration.getOrCreateStreamConsumerArn();

        final String secondCall = consumerRegistration.getOrCreateStreamConsumerArn();

        assertThat(firstCall, equalTo(CONSUMER_ARN));
        assertThat(secondCall, equalTo(CONSUMER_ARN));

        verify(client, never()).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
    }

    @Test(expected = LimitExceededException.class)
    public void testDescribeStreamConsumerThrottled() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture =
                CompletableFuture.completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture = CompletableFuture.supplyAsync(() -> {
            throw LimitExceededException.builder().build();
        });

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class)))
                .thenReturn(dscFuture);

        try {
            consumerRegistration.getOrCreateStreamConsumerArn();
        } finally {
            verify(client, times(MAX_DSC_RETRIES)).describeStreamConsumer(any(DescribeStreamConsumerRequest.class));
        }
    }

    @Test(expected = DependencyException.class)
    public void testRegisterStreamConsumerThrottled() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture =
                CompletableFuture.completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture = CompletableFuture.supplyAsync(() -> {
            throw ResourceNotFoundException.builder().build();
        });
        final CompletableFuture<RegisterStreamConsumerResponse> rscFuture = CompletableFuture.supplyAsync(() -> {
            throw LimitExceededException.builder().build();
        });

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class)))
                .thenReturn(dscFuture);
        when(client.registerStreamConsumer(any(RegisterStreamConsumerRequest.class)))
                .thenReturn(rscFuture);

        try {
            consumerRegistration.getOrCreateStreamConsumerArn();
        } finally {
            verify(client, times(RSC_RETRIES)).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
        }
    }

    @Test
    public void testNewRegisterStreamConsumer() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture =
                CompletableFuture.completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> failureResponse = CompletableFuture.supplyAsync(() -> {
            throw ResourceNotFoundException.builder().build();
        });
        final CompletableFuture<DescribeStreamConsumerResponse> intermidateResponse =
                CompletableFuture.completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.CREATING));
        final CompletableFuture<DescribeStreamConsumerResponse> successResponse =
                CompletableFuture.completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.ACTIVE));
        final CompletableFuture<RegisterStreamConsumerResponse> rscFuture =
                CompletableFuture.completedFuture(createRegisterStreamConsumerResponse());

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class)))
                .thenReturn(failureResponse)
                .thenReturn(intermidateResponse)
                .thenReturn(successResponse);
        when(client.registerStreamConsumer(any(RegisterStreamConsumerRequest.class)))
                .thenReturn(rscFuture);

        final long startTime = System.currentTimeMillis();
        final String consumerArn = consumerRegistration.getOrCreateStreamConsumerArn();
        final long endTime = System.currentTimeMillis();

        assertThat(consumerArn, equalTo(CONSUMER_ARN));
        assertThat(endTime - startTime, greaterThanOrEqualTo(2 * BACKOFF_MILLIS));

        verify(client).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testStreamConsumerStuckInCreating() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture =
                CompletableFuture.completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture =
                CompletableFuture.completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.CREATING));

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class)))
                .thenReturn(dscFuture);

        try {
            consumerRegistration.getOrCreateStreamConsumerArn();
        } finally {
            // Verify that the call to DSC was made for the max retry attempts and one for the initial response object.
            verify(client, times(MAX_DSC_RETRIES + 1)).describeStreamConsumer(any(DescribeStreamConsumerRequest.class));
            verify(client, never()).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
        }
    }

    private DescribeStreamSummaryRequest createDescribeStreamSummaryRequest() {
        return DescribeStreamSummaryRequest.builder().streamName(STREAM_NAME).build();
    }

    private DescribeStreamSummaryResponse createDescribeStreamSummaryResponse() {
        return DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(StreamDescriptionSummary.builder()
                        .streamName(STREAM_NAME)
                        .streamARN(STREAM_ARN)
                        .streamStatus(StreamStatus.ACTIVE)
                        .build())
                .build();
    }

    private DescribeStreamConsumerRequest createDescribeStreamConsumerRequest(final String consumerArn) {
        if (StringUtils.isEmpty(consumerArn)) {
            return DescribeStreamConsumerRequest.builder()
                    .streamARN(STREAM_ARN)
                    .consumerName(CONSUMER_NAME)
                    .build();
        }
        return DescribeStreamConsumerRequest.builder().consumerARN(consumerArn).build();
    }

    private DescribeStreamConsumerResponse createDescribeStreamConsumerResponse(final ConsumerStatus status) {
        return DescribeStreamConsumerResponse.builder()
                .consumerDescription(ConsumerDescription.builder()
                        .consumerStatus(status)
                        .consumerARN(CONSUMER_ARN)
                        .consumerName(CONSUMER_NAME)
                        .build())
                .build();
    }

    private RegisterStreamConsumerRequest createRegisterStreamConsumerRequest() {
        return RegisterStreamConsumerRequest.builder()
                .streamARN(STREAM_ARN)
                .consumerName(CONSUMER_NAME)
                .build();
    }

    private RegisterStreamConsumerResponse createRegisterStreamConsumerResponse() {
        return RegisterStreamConsumerResponse.builder()
                .consumer(Consumer.builder()
                        .consumerName(CONSUMER_NAME)
                        .consumerARN(CONSUMER_ARN)
                        .consumerStatus(ConsumerStatus.CREATING)
                        .build())
                .build();
    }
}
