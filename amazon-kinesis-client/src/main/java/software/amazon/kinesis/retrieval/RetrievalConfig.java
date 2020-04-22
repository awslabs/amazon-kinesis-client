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

package software.amazon.kinesis.retrieval;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.utils.Either;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

/**
 * Used by the KCL to configure the retrieval of records from Kinesis.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
@Accessors(fluent = true)
public class RetrievalConfig {
    /**
     * User agent set when Amazon Kinesis Client Library makes AWS requests.
     */
    public static final String KINESIS_CLIENT_LIB_USER_AGENT = "amazon-kinesis-client-library-java";

    public static final String KINESIS_CLIENT_LIB_USER_AGENT_VERSION = "2.2.10-SNAPSHOT";

    /**
     * Client used to make calls to Kinesis for records retrieval
     */
    @NonNull
    private final KinesisAsyncClient kinesisClient;

    @NonNull
    private final String applicationName;


    /**
     * AppStreamTracker either for multi stream tracking or single stream
     */
    private Either<MultiStreamTracker, StreamConfig> appStreamTracker;

    /**
     * Backoff time between consecutive ListShards calls.
     *
     * <p>
     * Default value: 1500L
     * </p>
     */
    private long listShardsBackoffTimeInMillis = 1500L;

    /**
     * Max number of retries for ListShards when throttled/exception is thrown.
     *
     * <p>
     * Default value: 50
     * </p>
     */
    private int maxListShardsRetryAttempts = 50;

    /**
     * The location in the shard from which the KinesisClientLibrary will start fetching records from
     * when the application starts for the first time and there is no checkpoint for the shard.
     *
     * <p>
     * Default value: {@link InitialPositionInStream#LATEST}
     * </p>
     */
    private InitialPositionInStreamExtended initialPositionInStreamExtended = InitialPositionInStreamExtended
            .newInitialPosition(InitialPositionInStream.LATEST);

    private RetrievalSpecificConfig retrievalSpecificConfig;

    private RetrievalFactory retrievalFactory;

    public RetrievalConfig(@NonNull KinesisAsyncClient kinesisAsyncClient, @NonNull String streamName,
                           @NonNull String applicationName) {
        this.kinesisClient = kinesisAsyncClient;
        this.appStreamTracker = Either
                .right(new StreamConfig(StreamIdentifier.singleStreamInstance(streamName), initialPositionInStreamExtended));
        this.applicationName = applicationName;
    }

    public RetrievalConfig(@NonNull KinesisAsyncClient kinesisAsyncClient, @NonNull MultiStreamTracker multiStreamTracker,
                           @NonNull String applicationName) {
        this.kinesisClient = kinesisAsyncClient;
        this.appStreamTracker = Either.left(multiStreamTracker);
        this.applicationName = applicationName;
    }

    public RetrievalConfig initialPositionInStreamExtended(InitialPositionInStreamExtended initialPositionInStreamExtended) {
        final StreamConfig[] streamConfig = new StreamConfig[1];
        this.appStreamTracker.apply(multiStreamTracker -> {
            throw new IllegalArgumentException(
                    "Cannot set initialPositionInStreamExtended when multiStreamTracker is set");
        }, sc -> streamConfig[0] = sc);
        this.appStreamTracker = Either
                .right(new StreamConfig(streamConfig[0].streamIdentifier(), initialPositionInStreamExtended));
        return this;
    }

    public RetrievalFactory retrievalFactory() {
        if (retrievalFactory == null) {
            if (retrievalSpecificConfig == null) {
                retrievalSpecificConfig = new FanOutConfig(kinesisClient())
                        .applicationName(applicationName());
                retrievalSpecificConfig = appStreamTracker.map(multiStreamTracker -> retrievalSpecificConfig,
                        streamConfig -> ((FanOutConfig) retrievalSpecificConfig).streamName(streamConfig.streamIdentifier().streamName()));
            }

            retrievalFactory = retrievalSpecificConfig.retrievalFactory();
        }
        validateConfig();
        return retrievalFactory;
    }

    private void validateConfig() {
        boolean isPollingConfig = retrievalSpecificConfig instanceof PollingConfig;
        boolean isInvalidPollingConfig = isPollingConfig && appStreamTracker.map(multiStreamTracker ->
                        ((PollingConfig) retrievalSpecificConfig).streamName() != null,
                streamConfig ->
                        streamConfig.streamIdentifier() == null || streamConfig.streamIdentifier().streamName() == null);

        if(isInvalidPollingConfig) {
            throw new IllegalArgumentException("Invalid config: multistream enabled with streamName or single stream with no streamName");
        }
    }
}
