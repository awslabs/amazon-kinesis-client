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

package software.amazon.kinesis.retrieval.polling;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.RetrievalSpecificConfig;

@Accessors(fluent = true)
@Getter
@Setter
@ToString
@EqualsAndHashCode
@Slf4j
public class PollingConfig implements RetrievalSpecificConfig {

    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

    public static final int DEFAULT_MAX_RECORDS = 10000;

    public static final long MIN_IDLE_MILLIS_BETWEEN_READS = 200L;

    /**
     * Configurable functional interface to override the existing DataFetcher.
     */
    Function<DataFetcherProviderConfig, DataFetcher> dataFetcherProvider;
    /**
     * Name of the Kinesis stream.
     */
    private String streamName;

    private boolean usePollingConfigIdleTimeValue;

    /**
     * @param kinesisClient Client used to access Kinesis services.
     */
    public PollingConfig(KinesisAsyncClient kinesisClient) {
        this.kinesisClient = kinesisClient;
    }

    /**
     * Client used to access to Kinesis service.
     */
    @NonNull
    private final KinesisAsyncClient kinesisClient;

    /**
     * Max records to fetch from Kinesis in a single GetRecords call.
     *
     * <p>
     * Default value: 10000
     * </p>
     */
    private int maxRecords = DEFAULT_MAX_RECORDS;

    /**
     * @param streamName    Name of Kinesis stream.
     * @param kinesisClient Client used to access Kinesis serivces.
     */
    public PollingConfig(String streamName, KinesisAsyncClient kinesisClient) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
    }

    /**
     * The value for how long the ShardConsumer should sleep in between calls to
     * {@link KinesisAsyncClient#getRecords(GetRecordsRequest)}.
     *
     * If this is not set using {@link PollingConfig#idleTimeBetweenReadsInMillis},
     * it defaults to 1500 ms.
     *
     * <p>
     * Default value: 1500L
     * </p>
     */
    @Setter(AccessLevel.NONE)
    private long idleTimeBetweenReadsInMillis = 1500L;

    /**
     * Time to wait in seconds before the worker retries to get a record.
     *
     * <p>
     * Default value: {@link Optional#empty()}
     * </p>
     */
    private Optional<Integer> retryGetRecordsInSeconds = Optional.empty();

    /**
     * The max number of threads in the records thread pool.
     *
     * <p>
     * Default value: {@link Optional#empty()}
     * </p>
     */
    private Optional<Integer> maxGetRecordsThreadPool = Optional.empty();

    /**
     * The factory that creates the RecordsPublisher used to records from Kinesis.
     *
     * <p>
     * Default value: {@link SimpleRecordsFetcherFactory}
     * </p>
     */
    private RecordsFetcherFactory recordsFetcherFactory = new SimpleRecordsFetcherFactory();

    /**
     * @Deprecated Use {@link PollingConfig#idleTimeBetweenReadsInMillis} instead
     */
    @Deprecated
    public void setIdleTimeBetweenReadsInMillis(long idleTimeBetweenReadsInMillis) {
        idleTimeBetweenReadsInMillis(idleTimeBetweenReadsInMillis);
    }

    /**
     * Set the value for how long the ShardConsumer should sleep in between calls to
     * {@link KinesisAsyncClient#getRecords(GetRecordsRequest)}. If this is not specified here the value provided in
     * {@link RecordsFetcherFactory} will be used. Cannot set value below MIN_IDLE_MILLIS_BETWEEN_READS.
     */
    public PollingConfig idleTimeBetweenReadsInMillis(long idleTimeBetweenReadsInMillis) {
        if (idleTimeBetweenReadsInMillis < MIN_IDLE_MILLIS_BETWEEN_READS) {
            log.warn(
                    "idleTimeBetweenReadsInMillis must be greater than or equal to {} but current value is {}."
                            + " Defaulting to minimum {}.",
                    MIN_IDLE_MILLIS_BETWEEN_READS,
                    idleTimeBetweenReadsInMillis,
                    MIN_IDLE_MILLIS_BETWEEN_READS);
            idleTimeBetweenReadsInMillis = MIN_IDLE_MILLIS_BETWEEN_READS;
        }
        usePollingConfigIdleTimeValue = true;
        this.idleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis;
        return this;
    }

    public PollingConfig maxRecords(int maxRecords) {
        if (maxRecords > DEFAULT_MAX_RECORDS) {
            throw new IllegalArgumentException("maxRecords must be less than or equal to " + DEFAULT_MAX_RECORDS
                    + " but current value is " + maxRecords());
        }
        this.maxRecords = maxRecords;
        return this;
    }

    /**
     * The maximum time to wait for a future request from Kinesis to complete
     */
    private Duration kinesisRequestTimeout = DEFAULT_REQUEST_TIMEOUT;

    @Override
    public RetrievalFactory retrievalFactory() {
        // Prioritize the PollingConfig specified value if its updated.
        if (usePollingConfigIdleTimeValue) {
            recordsFetcherFactory.idleMillisBetweenCalls(idleTimeBetweenReadsInMillis);
        }
        return new SynchronousBlockingRetrievalFactory(
                streamName(),
                kinesisClient(),
                recordsFetcherFactory,
                maxRecords(),
                kinesisRequestTimeout,
                dataFetcherProvider);
    }

    @Override
    public void validateState(final boolean isMultiStream) {
        if (isMultiStream) {
            if (streamName() != null) {
                throw new IllegalArgumentException(
                        "PollingConfig must not have streamName configured in multi-stream mode");
            }
        }
    }
}
