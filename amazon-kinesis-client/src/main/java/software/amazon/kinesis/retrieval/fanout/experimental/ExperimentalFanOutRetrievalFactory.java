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
package software.amazon.kinesis.retrieval.fanout.experimental;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientExperimental;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalFactory;

/**
 * Supports validating that records did originate from the shard.
 *
 * <h2><strong>This is an experimental class and may be removed at any time</strong></h2>
 */
@RequiredArgsConstructor
@KinesisClientExperimental
public class ExperimentalFanOutRetrievalFactory implements RetrievalFactory {
    private final KinesisAsyncClient kinesisClient;
    private final String consumerArn;

    @Override
    public GetRecordsRetrievalStrategy createGetRecordsRetrievalStrategy(ShardInfo shardInfo, MetricsFactory metricsFactory) {
        return null;
    }

    @Override
    public RecordsPublisher createGetRecordsCache(ShardInfo shardInfo, MetricsFactory metricsFactory) {
        return new ExperimentalFanOutRecordsPublisher(kinesisClient, shardInfo.shardId(), consumerArn);
    }
}
