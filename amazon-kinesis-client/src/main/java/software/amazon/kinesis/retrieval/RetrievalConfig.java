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

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.utils.Either;
import software.amazon.kinesis.common.DeprecationUtils;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;

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

    public static final String KINESIS_CLIENT_LIB_USER_AGENT_VERSION = "3.0.0";

    /**
     * Client used to make calls to Kinesis for records retrieval
     */
    @NonNull
    private final KinesisAsyncClient kinesisClient;

    @NonNull
    private final String applicationName;

    /**
     * Glue Schema Registry Deserializer instance.
     * If this instance is set, KCL will try to decode messages that might be
     * potentially encoded with Glue Schema Registry Serializer.
     */
    private GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer = null;

    /**
     * AppStreamTracker either for multi stream tracking or single stream
     *
     * @deprecated Both single- and multi-stream support is now provided by {@link StreamTracker}.
     * @see #streamTracker
     */
    private Either<MultiStreamTracker, StreamConfig> appStreamTracker;

    /**
     * Stream(s) to be consumed by this KCL application.
     */
    private StreamTracker streamTracker;

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
     *
     * @deprecated Initial stream position is now handled by {@link StreamTracker}.
     * @see StreamTracker#orphanedStreamInitialPositionInStream()
     * @see StreamTracker#createStreamConfig(StreamIdentifier)
     */
    @Deprecated
    private InitialPositionInStreamExtended initialPositionInStreamExtended =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

    private RetrievalSpecificConfig retrievalSpecificConfig;

    private RetrievalFactory retrievalFactory;

    public RetrievalConfig(
            @NonNull KinesisAsyncClient kinesisAsyncClient,
            @NonNull String streamName,
            @NonNull String applicationName) {
        this(kinesisAsyncClient, new SingleStreamTracker(streamName), applicationName);
    }

    public RetrievalConfig(
            @NonNull KinesisAsyncClient kinesisAsyncClient, @NonNull Arn streamArn, @NonNull String applicationName) {
        this(kinesisAsyncClient, new SingleStreamTracker(streamArn), applicationName);
    }

    public RetrievalConfig(
            @NonNull KinesisAsyncClient kinesisAsyncClient,
            @NonNull StreamTracker streamTracker,
            @NonNull String applicationName) {
        this.kinesisClient = kinesisAsyncClient;
        this.streamTracker = streamTracker;
        this.applicationName = applicationName;
        this.appStreamTracker = DeprecationUtils.convert(
                streamTracker,
                singleStreamTracker -> singleStreamTracker.streamConfigList().get(0));
    }

    /**
     * Convenience method to reconfigure the embedded {@link StreamTracker},
     * but only when <b>not</b> in multi-stream mode.
     *
     * @param initialPositionInStreamExtended
     *
     * @deprecated Initial stream position is now handled by {@link StreamTracker}.
     * @see StreamTracker#orphanedStreamInitialPositionInStream()
     * @see StreamTracker#createStreamConfig(StreamIdentifier)
     */
    @Deprecated
    public RetrievalConfig initialPositionInStreamExtended(
            InitialPositionInStreamExtended initialPositionInStreamExtended) {
        if (streamTracker().isMultiStream()) {
            throw new IllegalArgumentException(
                    "Cannot set initialPositionInStreamExtended when multiStreamTracker is set");
        }

        final StreamIdentifier streamIdentifier = getSingleStreamIdentifier();
        final StreamConfig updatedConfig = new StreamConfig(streamIdentifier, initialPositionInStreamExtended);
        streamTracker = new SingleStreamTracker(streamIdentifier, updatedConfig);
        appStreamTracker = Either.right(updatedConfig);
        return this;
    }

    public RetrievalConfig retrievalSpecificConfig(RetrievalSpecificConfig retrievalSpecificConfig) {
        retrievalSpecificConfig.validateState(streamTracker.isMultiStream());
        this.retrievalSpecificConfig = retrievalSpecificConfig;
        return this;
    }

    public RetrievalFactory retrievalFactory() {
        if (retrievalFactory == null) {
            if (retrievalSpecificConfig == null) {
                final FanOutConfig fanOutConfig = new FanOutConfig(kinesisClient()).applicationName(applicationName());
                if (!streamTracker.isMultiStream()) {
                    final String streamName = getSingleStreamIdentifier().streamName();
                    fanOutConfig.streamName(streamName);
                }
                retrievalSpecificConfig(fanOutConfig);
            }
            retrievalFactory = retrievalSpecificConfig.retrievalFactory();
        }
        return retrievalFactory;
    }

    /**
     * Convenience method to return the {@link StreamIdentifier} from a
     * single-stream tracker.
     */
    private StreamIdentifier getSingleStreamIdentifier() {
        return streamTracker.streamConfigList().get(0).streamIdentifier();
    }
}
