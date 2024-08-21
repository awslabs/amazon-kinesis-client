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

import java.util.concurrent.ExecutionException;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.KinesisRequestsBuilder;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.ConsumerRegistration;

/**
 *
 */
@RequiredArgsConstructor
@Slf4j
@Accessors(fluent = true)
@KinesisClientInternalApi
public class FanOutConsumerRegistration implements ConsumerRegistration {
    @NonNull
    private final KinesisAsyncClient kinesisClient;

    private final String streamName;

    @NonNull
    private final String streamConsumerName;

    private final int maxDescribeStreamSummaryRetries;
    private final int maxDescribeStreamConsumerRetries;
    private final int registerStreamConsumerRetries;
    private final long retryBackoffMillis;

    private String streamArn;

    @Setter(AccessLevel.PRIVATE)
    private String streamConsumerArn;

    /**
     * @inheritDoc
     */
    @Override
    public String getOrCreateStreamConsumerArn() throws DependencyException {
        if (StringUtils.isEmpty(streamConsumerArn)) {
            DescribeStreamConsumerResponse response = null;

            // 1. Check if consumer exists
            try {
                response = describeStreamConsumer();
            } catch (ResourceNotFoundException e) {
                log.info("{} : StreamConsumer not found, need to create it.", streamName);
            }

            // 2. If not, register consumer
            if (response == null) {
                LimitExceededException finalException = null;
                int retries = registerStreamConsumerRetries;
                try {
                    while (retries > 0) {
                        finalException = null;
                        try {
                            final RegisterStreamConsumerResponse registerResponse = registerStreamConsumer();
                            streamConsumerArn(registerResponse.consumer().consumerARN());
                            break;
                        } catch (LimitExceededException e) {
                            // TODO: Figure out internal service exceptions
                            log.debug("{} : RegisterStreamConsumer call got throttled will retry.", streamName);
                            finalException = e;
                        }
                        retries--;
                    }

                    // All calls got throttled, returning.
                    if (finalException != null) {
                        throw new DependencyException(finalException);
                    }
                } catch (ResourceInUseException e) {
                    // Consumer is present, call DescribeStreamConsumer
                    log.debug(
                            "{} : Got ResourceInUseException consumer exists, will call DescribeStreamConsumer again.",
                            streamName);
                    response = describeStreamConsumer();
                }
            }

            // Update consumer arn, if describe was successful.
            if (response != null) {
                streamConsumerArn(response.consumerDescription().consumerARN());
            }

            // Check if consumer is active before proceeding
            waitForActive();
        }
        return streamConsumerArn;
    }

    private RegisterStreamConsumerResponse registerStreamConsumer() throws DependencyException {
        final AWSExceptionManager exceptionManager = createExceptionManager();
        try {
            final RegisterStreamConsumerRequest request = KinesisRequestsBuilder.registerStreamConsumerRequestBuilder()
                    .streamARN(streamArn())
                    .consumerName(streamConsumerName)
                    .build();
            return kinesisClient.registerStreamConsumer(request).get();
        } catch (ExecutionException e) {
            throw exceptionManager.apply(e.getCause());
        } catch (InterruptedException e) {
            throw new DependencyException(e);
        }
    }

    private DescribeStreamConsumerResponse describeStreamConsumer() throws DependencyException {
        final DescribeStreamConsumerRequest.Builder requestBuilder =
                KinesisRequestsBuilder.describeStreamConsumerRequestBuilder();
        final DescribeStreamConsumerRequest request;

        if (StringUtils.isEmpty(streamConsumerArn)) {
            request = requestBuilder
                    .streamARN(streamArn())
                    .consumerName(streamConsumerName)
                    .build();
        } else {
            request = requestBuilder.consumerARN(streamConsumerArn).build();
        }

        final ServiceCallerSupplier<DescribeStreamConsumerResponse> dsc =
                () -> kinesisClient.describeStreamConsumer(request).get();

        return retryWhenThrottled(dsc, maxDescribeStreamConsumerRetries, "DescribeStreamConsumer");
    }

    private void waitForActive() throws DependencyException {
        ConsumerStatus status = null;

        int retries = maxDescribeStreamConsumerRetries;

        try {
            while (!ConsumerStatus.ACTIVE.equals(status) && retries > 0) {
                status = describeStreamConsumer().consumerDescription().consumerStatus();
                retries--;
                log.info("{} : Waiting for StreamConsumer {} to have ACTIVE status...", streamName, streamConsumerName);
                Thread.sleep(retryBackoffMillis);
            }
        } catch (InterruptedException ie) {
            log.debug("{} : Thread was interrupted while fetching StreamConsumer status, moving on.", streamName);
        }

        if (!ConsumerStatus.ACTIVE.equals(status)) {
            final String message = String.format(
                    "%s : Status of StreamConsumer %s, was not ACTIVE after all retries. Was instead %s.",
                    streamName, streamConsumerName, status);
            log.error(message);
            throw new IllegalStateException(message);
        }
    }

    private String streamArn() throws DependencyException {
        if (StringUtils.isEmpty(streamArn)) {
            final DescribeStreamSummaryRequest request = KinesisRequestsBuilder.describeStreamSummaryRequestBuilder()
                    .streamName(streamName)
                    .build();
            final ServiceCallerSupplier<String> dss = () -> kinesisClient
                    .describeStreamSummary(request)
                    .get()
                    .streamDescriptionSummary()
                    .streamARN();

            streamArn = retryWhenThrottled(dss, maxDescribeStreamSummaryRetries, "DescribeStreamSummary");
        }

        return streamArn;
    }

    @FunctionalInterface
    private interface ServiceCallerSupplier<T> {
        T get() throws ExecutionException, InterruptedException;
    }

    private <T> T retryWhenThrottled(
            @NonNull final ServiceCallerSupplier<T> retriever, final int maxRetries, @NonNull final String apiName)
            throws DependencyException {
        final AWSExceptionManager exceptionManager = createExceptionManager();

        LimitExceededException finalException = null;

        int retries = maxRetries;
        while (retries > 0) {
            try {
                try {
                    return retriever.get();
                } catch (ExecutionException e) {
                    throw exceptionManager.apply(e.getCause());
                } catch (InterruptedException e) {
                    throw new DependencyException(e);
                }
            } catch (LimitExceededException e) {
                log.info("{} : Throttled while calling {} API, will backoff.", streamName, apiName);
                try {
                    Thread.sleep(retryBackoffMillis + (long) (Math.random() * 100));
                } catch (InterruptedException ie) {
                    log.debug("Sleep interrupted, shutdown invoked.");
                }
                finalException = e;
            }
            retries--;
        }

        if (finalException == null) {
            throw new IllegalStateException(String.format(
                    "%s : Finished all retries and no exception was caught while calling %s", streamName, apiName));
        }

        throw finalException;
    }

    private AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(LimitExceededException.class, t -> t);
        exceptionManager.add(ResourceInUseException.class, t -> t);
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(KinesisException.class, t -> t);

        return exceptionManager;
    }
}
