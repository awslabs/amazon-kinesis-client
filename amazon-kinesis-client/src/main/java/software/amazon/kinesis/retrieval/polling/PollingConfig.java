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
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
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
public class PollingConfig implements RetrievalSpecificConfig {

    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Configurable functional interface to override the existing DataFetcher.
     */
    Function<DataFetcherProviderConfig, DataFetcher> dataFetcherProvider;
    /**
     * Name of the Kinesis stream.
     *
     * @return String
     */
    private String streamName;

    /**
     * @param kinesisClient Client used to access Kinesis services.
     */
    public PollingConfig(KinesisAsyncClient kinesisClient) {
        this.kinesisClient = kinesisClient;
    }

    /**
     * Client used to access to Kinesis service.
     *
     * @return {@link KinesisAsyncClient}
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
    private int maxRecords = 10000;

    /**
     * @param streamName    Name of Kinesis stream.
     * @param kinesisClient Client used to access Kinesis serivces.
     */
    public PollingConfig(String streamName, KinesisAsyncClient kinesisClient) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
    }

    /**
     * The value for how long the ShardConsumer should sleep if no records are returned from the call to
     * {@link KinesisAsyncClient#getRecords(GetRecordsRequest)}.
     *
     * <p>
     * Default value: 1000L
     * </p>
     */
    private long idleTimeBetweenReadsInMillis = 1000L;

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
     * The maximum time to wait for a future request from Kinesis to complete
     */
    private Duration kinesisRequestTimeout = DEFAULT_REQUEST_TIMEOUT;

    @Override
    public RetrievalFactory retrievalFactory() {
        return new SynchronousBlockingRetrievalFactory(streamName(), kinesisClient(), recordsFetcherFactory,
                maxRecords(), kinesisRequestTimeout, dataFetcherProvider);
    }
}
