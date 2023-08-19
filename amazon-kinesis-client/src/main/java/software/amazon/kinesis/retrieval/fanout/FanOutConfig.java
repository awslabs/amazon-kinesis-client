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

import org.apache.commons.lang3.ObjectUtils;

import com.google.common.base.Preconditions;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.RetrievalSpecificConfig;

@Data
@Accessors(fluent = true)
public class FanOutConfig implements RetrievalSpecificConfig {

    /**
     * Client used for retrieval, and optional consumer creation
     */
    @NonNull
    private final KinesisAsyncClient kinesisClient;

    /**
     * The ARN of an already created consumer, if this is set no automatic consumer creation will be attempted.
     */
    private String consumerArn;

    /**
     * The name of the stream to create a consumer for.
     */
    private String streamName;

    /**
     * The name of the consumer to create. If this isn't set the {@link #applicationName} will be used.
     */
    private String consumerName;

    /**
     * The name of this application. Used as the name of the consumer unless {@link #consumerName} is set
     */
    private String applicationName;

    /**
     * The maximum number of retries for calling describe stream summary. Once exhausted the consumer creation/retrieval
     * will fail.
     */
    private int maxDescribeStreamSummaryRetries = 10;

    /**
     * The maximum number of retries for calling DescribeStreamConsumer. Once exhausted the consumer creation/retrieval
     * will fail.
     */
    private int maxDescribeStreamConsumerRetries = 10;

    /**
     * The maximum number of retries for calling RegisterStreamConsumer. Once exhausted the consumer creation/retrieval
     * will fail.
     */
    private int registerStreamConsumerRetries = 10;

    /**
     * The maximum amount of time that will be made between failed calls.
     */
    private long retryBackoffMillis = 1000;

    @Override
    public RetrievalFactory retrievalFactory() {
        return new FanOutRetrievalFactory(kinesisClient, streamName, consumerArn, this::getOrCreateConsumerArn);
    }

    @Override
    public void validateState(final boolean isMultiStream) {
        if (isMultiStream) {
            if ((streamName() != null) || (consumerArn() != null)) {
                throw new IllegalArgumentException(
                        "FanOutConfig must not have streamName/consumerArn configured in multi-stream mode");
            }
        }
    }

    private String getOrCreateConsumerArn(String streamName) {
        FanOutConsumerRegistration registration = createConsumerRegistration(streamName);
        try {
            return registration.getOrCreateStreamConsumerArn();
        } catch (DependencyException e) {
            throw new RuntimeException(e);
        }
    }

    private FanOutConsumerRegistration createConsumerRegistration(String streamName) {
        String consumerToCreate = ObjectUtils.firstNonNull(consumerName(), applicationName());
        return createConsumerRegistration(kinesisClient(),
                Preconditions.checkNotNull(streamName, "streamName must be set for consumer creation"),
                Preconditions.checkNotNull(consumerToCreate,
                        "applicationName or consumerName must be set for consumer creation"));

    }

    protected FanOutConsumerRegistration createConsumerRegistration(KinesisAsyncClient client, String stream,
                                                                    String consumerToCreate) {
        return new FanOutConsumerRegistration(client, stream, consumerToCreate, maxDescribeStreamSummaryRetries(),
                maxDescribeStreamConsumerRetries(), registerStreamConsumerRetries(), retryBackoffMillis());
    }

}
