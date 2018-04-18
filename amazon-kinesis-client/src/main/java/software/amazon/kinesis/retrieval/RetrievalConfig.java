/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License. 
 */

package software.amazon.kinesis.retrieval;

import java.util.Optional;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.kinesis.lifecycle.ShardConsumer;

/**
 * Used by the KCL to configure the retrieval of records from Kinesis.
 */
@Data
@Accessors(fluent = true)
public class RetrievalConfig {
    /**
     * User agent set when Amazon Kinesis Client Library makes AWS requests.
     */
    public static final String KINESIS_CLIENT_LIB_USER_AGENT = "amazon-kinesis-client-library-java-1.9.0";

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
     * @return {@link AmazonKinesis}
     */
    @NonNull
    private final AmazonKinesis amazonKinesis;

    /**
     * Max records to fetch from Kinesis in a single GetRecords call.
     *
     * <p>Default value: 10000</p>
     */
    private int maxRecords = 10000;

    /**
     * The value for how long the {@link ShardConsumer} should sleep if no records are returned from the call to
     * {@link com.amazonaws.services.kinesis.AmazonKinesis#getRecords(com.amazonaws.services.kinesis.model.GetRecordsRequest)}.
     *
     * <p>Default value: 1000L</p>
     */
    private long idleTimeBetweenReadsInMillis = 1000L;

    /**
     * Time to wait in seconds before the worker retries to get a record.
     *
     * <p>Default value: {@link Optional#empty()}</p>
     */
    private Optional<Integer> retryGetRecordsInSeconds = Optional.empty();

    /**
     * The max number of threads in the getRecords thread pool.
     *
     * <p>Default value: {@link Optional#empty()}</p>
     */
    private Optional<Integer> maxGetRecordsThreadPool = Optional.empty();

    /**
     * The factory that creates the {@link GetRecordsCache} used to getRecords from Kinesis.
     *
     * <p>Default value: {@link SimpleRecordsFetcherFactory}</p>
     */
    private RecordsFetcherFactory recordsFetcherFactory = new SimpleRecordsFetcherFactory();

    /**
     * Backoff time between consecutive ListShards calls.
     *
     * <p>Default value: 1500L</p>
     */
    private long listShardsBackoffTimeInMillis = 1500L;

    /**
     * Max number of retries for ListShards when throttled/exception is thrown.
     *
     * <p>Default value: 50</p>
     */
    private int maxListShardsRetryAttempts = 50;

    /**
     * The location in the shard from which the KinesisClientLibrary will start fetching records from
     * when the application starts for the first time and there is no checkpoint for the shard.
     *
     * <p>Default value: {@link InitialPositionInStream#LATEST}</p>
     */
    private InitialPositionInStreamExtended initialPositionInStreamExtended =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

    private DataFetchingStrategy dataFetchingStrategy = DataFetchingStrategy.DEFAULT;

    private RetrievalFactory retrievalFactory;

    public RetrievalFactory retrievalFactory() {
        if (retrievalFactory == null) {
            retrievalFactory = new SynchronousBlockingRetrievalFactory(streamName(), amazonKinesis(),
                    recordsFetcherFactory,
                    listShardsBackoffTimeInMillis(), maxListShardsRetryAttempts(), maxRecords());
        }
        return retrievalFactory;
    }
}
