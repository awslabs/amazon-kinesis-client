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

package software.amazon.kinesis.retrieval.polling;

import java.util.Optional;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.RetrievalSpecificConfig;

@Accessors(fluent = true)
@Data
@Getter
public class PollingConfig implements RetrievalSpecificConfig {

    /**
     * Name of the Kinesis stream.
     *
     * @return String
     */
    @NonNull
    private final String streamName;

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

    @Override
    public RetrievalFactory retrievalFactory() {
        return new SynchronousBlockingRetrievalFactory(streamName(), kinesisClient(), recordsFetcherFactory,
                maxRecords());
    }
}
