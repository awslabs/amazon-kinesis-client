/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.kinesis.retrieval.fanout;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        consumerRegistration = new FanOutConsumerRegistration(client, STREAM_NAME, CONSUMER_NAME, MAX_DSS_RETRIES,
                MAX_DSC_RETRIES, RSC_RETRIES, BACKOFF_MILLIS);
    }

    @Test
    public void testConsumerAlreadyExists() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture = CompletableFuture
                .completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture = CompletableFuture
                .completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.ACTIVE));

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class))).thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenReturn(dscFuture);

        final String consumerArn = consumerRegistration.getOrCreateStreamConsumerArn();

        assertThat(consumerArn, equalTo(CONSUMER_ARN));

        verify(client).describeStreamConsumer(eq(createDescribeStreamConsumerRequest(null)));
        verify(client).describeStreamSummary(eq(createDescribeStreamSummaryRequest()));
        verify(client, never()).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
    }

    @Test
    public void testConsumerAlreadyExistsMultipleCalls() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture = CompletableFuture
                .completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture = CompletableFuture
                .completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.ACTIVE));

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class))).thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenReturn(dscFuture);

        final String firstCall = consumerRegistration.getOrCreateStreamConsumerArn();

        final String secondCall = consumerRegistration.getOrCreateStreamConsumerArn();

        assertThat(firstCall, equalTo(CONSUMER_ARN));
        assertThat(secondCall, equalTo(CONSUMER_ARN));

        verify(client).describeStreamConsumer(eq(createDescribeStreamConsumerRequest(null)));
        verify(client).describeStreamSummary(eq(createDescribeStreamSummaryRequest()));
        verify(client, never()).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
    }

    @Test(expected = LimitExceededException.class)
    public void testDescribeStreamConsumerThrottled() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture = CompletableFuture
                .completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture = CompletableFuture.supplyAsync(() -> {
            throw LimitExceededException.builder().build();
        });

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class))).thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenReturn(dscFuture);

        try {
            consumerRegistration.getOrCreateStreamConsumerArn();
        } finally {
            verify(client).describeStreamSummary(eq(createDescribeStreamSummaryRequest()));
            verify(client, times(MAX_DSC_RETRIES))
                    .describeStreamConsumer(eq(createDescribeStreamConsumerRequest(null)));
        }
    }

    @Test(expected = DependencyException.class)
    public void testRegisterStreamConsumerThrottled() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture = CompletableFuture
                .completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture = CompletableFuture.supplyAsync(() -> {
            throw ResourceNotFoundException.builder().build();
        });
        final CompletableFuture<RegisterStreamConsumerResponse> rscFuture = CompletableFuture.supplyAsync(() -> {
            throw LimitExceededException.builder().build();
        });

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class))).thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenReturn(dscFuture);
        when(client.registerStreamConsumer(any(RegisterStreamConsumerRequest.class))).thenReturn(rscFuture);

        try {
            consumerRegistration.getOrCreateStreamConsumerArn();
        } finally {
            verify(client, times(RSC_RETRIES))
                    .registerStreamConsumer(eq(createRegisterStreamConsumerRequest()));
            // Verify that DescribeStreamConsumer was called for at least RegisterStreamConsumer retries + 1 at start.
            verify(client).describeStreamConsumer(eq(createDescribeStreamConsumerRequest(null)));
        }
    }

    @Test
    public void testNewRegisterStreamConsumer() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture = CompletableFuture
                .completedFuture(createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> failureResponse = CompletableFuture.supplyAsync(() -> {
            throw ResourceNotFoundException.builder().build();
        });
        final CompletableFuture<DescribeStreamConsumerResponse> intermidateResponse = CompletableFuture
                .completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.CREATING));
        final CompletableFuture<DescribeStreamConsumerResponse> successResponse = CompletableFuture
                .completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.ACTIVE));
        final CompletableFuture<RegisterStreamConsumerResponse> rscFuture = CompletableFuture
                .completedFuture(createRegisterStreamConsumerResponse());

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class))).thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenReturn(failureResponse)
                .thenReturn(intermidateResponse).thenReturn(successResponse);
        when(client.registerStreamConsumer(any(RegisterStreamConsumerRequest.class))).thenReturn(rscFuture);

        final String consumerArn = consumerRegistration.getOrCreateStreamConsumerArn();

        assertThat(consumerArn, equalTo(CONSUMER_ARN));

        verify(client).registerStreamConsumer(eq(createRegisterStreamConsumerRequest()));
        verify(client).describeStreamSummary(eq(createDescribeStreamSummaryRequest()));
        verify(client).describeStreamConsumer(eq(createDescribeStreamConsumerRequest(null)));
        verify(client, times(2))
                .describeStreamConsumer(eq(createDescribeStreamConsumerRequest(CONSUMER_ARN)));
    }

    @Test(expected = IllegalStateException.class)
    public void testStreamConsumerStuckInCreating() throws Exception {
        final CompletableFuture<DescribeStreamSummaryResponse> dssFuture = CompletableFuture.completedFuture(
                createDescribeStreamSummaryResponse());
        final CompletableFuture<DescribeStreamConsumerResponse> dscFuture = CompletableFuture
                .completedFuture(createDescribeStreamConsumerResponse(ConsumerStatus.CREATING));

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class))).thenReturn(dssFuture);
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenReturn(dscFuture);

        try {
            consumerRegistration.getOrCreateStreamConsumerArn();
        } finally {
            verify(client).describeStreamSummary(eq(createDescribeStreamSummaryRequest()));
            // Verify that the call to DSC was made for the max retry attempts and one for the initial response object.
            verify(client).describeStreamConsumer(eq(createDescribeStreamConsumerRequest(null)));
            verify(client, times(MAX_DSC_RETRIES))
                    .describeStreamConsumer(eq(createDescribeStreamConsumerRequest(CONSUMER_ARN)));
            verify(client, never()).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
        }

    }

    private DescribeStreamSummaryRequest createDescribeStreamSummaryRequest() {
        return DescribeStreamSummaryRequest.builder().streamName(STREAM_NAME).build();
    }

    private DescribeStreamSummaryResponse createDescribeStreamSummaryResponse() {
        return DescribeStreamSummaryResponse.builder().streamDescriptionSummary(StreamDescriptionSummary.builder()
                .streamName(STREAM_NAME).streamARN(STREAM_ARN).streamStatus(StreamStatus.ACTIVE).build()).build();
    }

    private DescribeStreamConsumerRequest createDescribeStreamConsumerRequest(final String consumerArn) {
        if (StringUtils.isEmpty(consumerArn)) {
            return DescribeStreamConsumerRequest.builder().streamARN(STREAM_ARN).consumerName(CONSUMER_NAME).build();
        }
        return DescribeStreamConsumerRequest.builder().consumerARN(consumerArn).build();
    }

    private DescribeStreamConsumerResponse createDescribeStreamConsumerResponse(final ConsumerStatus status) {
        return DescribeStreamConsumerResponse.builder().consumerDescription(ConsumerDescription.builder()
                .consumerStatus(status).consumerARN(CONSUMER_ARN).consumerName(CONSUMER_NAME).build()).build();
    }

    private RegisterStreamConsumerRequest createRegisterStreamConsumerRequest() {
        return RegisterStreamConsumerRequest.builder().streamARN(STREAM_ARN).consumerName(CONSUMER_NAME).build();
    }

    private RegisterStreamConsumerResponse createRegisterStreamConsumerResponse() {
        return RegisterStreamConsumerResponse.builder().consumer(Consumer.builder().consumerName(CONSUMER_NAME)
                .consumerARN(CONSUMER_ARN).consumerStatus(ConsumerStatus.CREATING).build()).build();
    }

}
