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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@RequiredArgsConstructor
@KinesisClientInternalApi
public class FanOutRetrievalFactory implements RetrievalFactory {

    private final KinesisAsyncClient kinesisClient;
    private final String defaultStreamName;
    private final String defaultConsumerArn;
    private final Function<String, String> consumerArnCreator;

    private final Map<StreamIdentifier, String> implicitConsumerArnTracker = new HashMap<>();

    @Override
    public RecordsPublisher createGetRecordsCache(@NonNull final ShardInfo shardInfo,
            @NonNull final StreamConfig streamConfig,
            @Nullable final MetricsFactory metricsFactory) {
        final Optional<String> streamIdentifierStr = shardInfo.streamIdentifierSerOpt();
        if (streamIdentifierStr.isPresent()) {
            return new FanOutRecordsPublisher(kinesisClient, shardInfo.shardId(),
                    getOrCreateConsumerArn(streamConfig.streamIdentifier(), streamConfig.consumerArn()),
                    streamIdentifierStr.get());
        } else {
            return new FanOutRecordsPublisher(kinesisClient, shardInfo.shardId(),
                    getOrCreateConsumerArn(streamConfig.streamIdentifier(), defaultConsumerArn));
        }
    }

    private String getOrCreateConsumerArn(StreamIdentifier streamIdentifier, String consumerArn) {
        return consumerArn != null ? consumerArn : implicitConsumerArnTracker
                    .computeIfAbsent(streamIdentifier, sId -> consumerArnCreator.apply(sId.streamName()));
    }
}
