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

package software.amazon.kinesis.retrieval.fanout;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalFactory;

@RequiredArgsConstructor
@KinesisClientInternalApi
public class FanOutRetrievalFactory implements RetrievalFactory {

    private final KinesisAsyncClient kinesisClient;
    private final String consumerArn;

    @Override
    public GetRecordsRetrievalStrategy createGetRecordsRetrievalStrategy(final ShardInfo shardInfo,
            final MetricsFactory metricsFactory) {
        return null;
    }

    @Override
    public RecordsPublisher createGetRecordsCache(@NonNull final ShardInfo shardInfo,
            final MetricsFactory metricsFactory) {
        return new FanOutRecordsPublisher(kinesisClient, shardInfo.shardId(), consumerArn);
    }
}
