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

package software.amazon.kinesis.leases;

import java.util.concurrent.ConcurrentMap;

import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.coordinator.DeletedStreamListProvider;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.metrics.MetricsFactory;

/**
 *
 */
public interface LeaseManagementFactory {
    LeaseCoordinator createLeaseCoordinator(MetricsFactory metricsFactory);

    default LeaseCoordinator createLeaseCoordinator(
            MetricsFactory metricsFactory, ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap) {
        throw new UnsupportedOperationException();
    }

    ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory);

    default ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory, StreamConfig streamConfig) {
        throw new UnsupportedOperationException();
    }

    default ShardSyncTaskManager createShardSyncTaskManager(
            MetricsFactory metricsFactory,
            StreamConfig streamConfig,
            DeletedStreamListProvider deletedStreamListProvider) {
        throw new UnsupportedOperationException("createShardSyncTaskManager method not implemented");
    }

    DynamoDBLeaseRefresher createLeaseRefresher();

    ShardDetector createShardDetector();

    default ShardDetector createShardDetector(StreamConfig streamConfig) {
        throw new UnsupportedOperationException();
    }

    LeaseCleanupManager createLeaseCleanupManager(MetricsFactory metricsFactory);
}
