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

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.metrics.MetricsFactory;

/**
 * Configuration needed for custom data fetchers
 */
@Data
public class KinesisDataFetcherProviderConfig implements DataFetcherProviderConfig {

    @NonNull
    private StreamIdentifier streamIdentifier;

    @NonNull
    private String shardId;

    @NonNull
    private MetricsFactory metricsFactory;

    @NonNull
    private Integer maxRecords;

    @NonNull
    private Duration kinesisRequestTimeout;

    @Accessors(fluent = true)
    private String consumerId;

    public KinesisDataFetcherProviderConfig(
            @NonNull StreamIdentifier streamIdentifier,
            @NonNull String shardId,
            @NonNull MetricsFactory metricsFactory,
            int maxRecords,
            @NonNull Duration kinesisRequestTimeout) {
        this(streamIdentifier, shardId, metricsFactory, maxRecords, kinesisRequestTimeout, "");
    }

    public KinesisDataFetcherProviderConfig(
            @NonNull StreamIdentifier streamIdentifier,
            @NotNull String shardId,
            @NonNull MetricsFactory metricsFactory,
            int maxRecords,
            @NotNull Duration kinesisRequestTimeout,
            String consumerId) {
        this.streamIdentifier = streamIdentifier;
        this.shardId = shardId;
        this.metricsFactory = metricsFactory;
        this.maxRecords = maxRecords;
        this.kinesisRequestTimeout = kinesisRequestTimeout;
        this.consumerId = consumerId;
    }
}
