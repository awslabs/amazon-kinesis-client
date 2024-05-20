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

import java.time.Duration;

import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.metrics.MetricsFactory;

public interface DataFetcherProviderConfig {

    /**
     * Gets stream identifier for dataFetcher.
     */
    StreamIdentifier getStreamIdentifier();

    /**
     * Gets shard id.
     */
    String getShardId();

    /**
     * Gets current instance of metrics factory.
     */
    MetricsFactory getMetricsFactory();

    /**
     * Gets current max records allowed to process at a given time.
     */
    Integer getMaxRecords();

    /**
     * Gets timeout for kinesis request.
     */
    Duration getKinesisRequestTimeout();
}
