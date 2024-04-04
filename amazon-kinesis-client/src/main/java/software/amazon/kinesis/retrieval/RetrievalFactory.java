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

import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;

/**
 *
 */
public interface RetrievalFactory {

    /**
     * @deprecated This method was only used by specific implementations of {@link RetrievalFactory} and should not be
     *             required to be implemented; will be removed in future versions.
     */
    @Deprecated
    default GetRecordsRetrievalStrategy createGetRecordsRetrievalStrategy(ShardInfo shardInfo, MetricsFactory metricsFactory) {
        throw new UnsupportedOperationException("This method is deprecated and should not be used.");
    }

    /**
     * Creates a {@link RecordsPublisher} instance to retrieve records for the specified shard.
     *
     * @param shardInfo The {@link ShardInfo} representing the shard for which records are to be retrieved.
     * @param metricsFactory The {@link MetricsFactory} for recording metrics.
     * @return A {@link RecordsPublisher} instance for retrieving records from the shard.
     */
    RecordsPublisher createGetRecordsCache(ShardInfo shardInfo, MetricsFactory metricsFactory);

    /**
     * @deprecated {@link ShardInfo} now includes a reference to {@link StreamConfig}.
     *             Please use {@link #createGetRecordsCache(ShardInfo, MetricsFactory)} instead.
     *             This method will be removed in future versions.
     */
    @Deprecated
    default RecordsPublisher createGetRecordsCache(ShardInfo shardInfo, StreamConfig streamConfig, MetricsFactory metricsFactory) {
        return createGetRecordsCache(shardInfo, metricsFactory);
    }
}
