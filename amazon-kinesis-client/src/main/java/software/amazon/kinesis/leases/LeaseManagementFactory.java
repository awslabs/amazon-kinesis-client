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

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.DeletedStreamListProvider;
import software.amazon.kinesis.coordinator.StreamInfoManager;
import software.amazon.kinesis.coordinator.streamInfo.StreamIdCacheManager;
import software.amazon.kinesis.coordinator.streamInfo.StreamIdOnboardingState;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoDAO;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoMode;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.metrics.MetricsFactory;

/**
 *
 */
public interface LeaseManagementFactory {
    LeaseCoordinator createLeaseCoordinator(MetricsFactory metricsFactory);

    @Deprecated
    default LeaseCoordinator createLeaseCoordinator(
            MetricsFactory metricsFactory, ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap) {
        throw new UnsupportedOperationException("Not implemented");
    }

    default LeaseCoordinator createLeaseCoordinator(
            MetricsFactory metricsFactory,
            ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap,
            StreamIdCacheManager streamIdCacheManager) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * @deprecated This method is never invoked, please remove implementation of this method
     *               as it will be removed in future releases.
     */
    @Deprecated
    default ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory) {
        throw new UnsupportedOperationException("Deprecated");
    }

    /**
     * @deprecated This method is never invoked, please remove implementation of this method
     *               as it will be removed in future releases.
     */
    @Deprecated
    default ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory, StreamConfig streamConfig) {
        throw new UnsupportedOperationException("Deprecated");
    }

    @Deprecated
    default ShardSyncTaskManager createShardSyncTaskManager(
            MetricsFactory metricsFactory,
            StreamConfig streamConfig,
            DeletedStreamListProvider deletedStreamListProvider) {
        throw new UnsupportedOperationException("createShardSyncTaskManager method not implemented");
    }

    default ShardSyncTaskManager createShardSyncTaskManager(
            MetricsFactory metricsFactory,
            StreamConfig streamConfig,
            DeletedStreamListProvider deletedStreamListProvider,
            StreamInfoManager streamInfoManager) {
        throw new UnsupportedOperationException("createShardSyncTaskManager method not implemented");
    }

    DynamoDBLeaseRefresher createLeaseRefresher();

    /**
     * @deprecated This method is never invoked, please remove implementation of this method
     *               as it will be removed in future releases.
     */
    @Deprecated
    default ShardDetector createShardDetector() {
        throw new UnsupportedOperationException("Deprecated");
    }

    default ShardDetector createShardDetector(StreamConfig streamConfig) {
        throw new UnsupportedOperationException("Not implemented");
    }

    LeaseCleanupManager createLeaseCleanupManager(MetricsFactory metricsFactory);

    StreamIdCacheManager createStreamIdCacheManager(
            StreamInfoDAO streamInfoDAO,
            Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            StreamIdOnboardingState streamIdOnboardingState,
            boolean isMultiStreamMode);

    StreamInfoManager createStreamInfoManager(
            Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            StreamInfoDAO streamInfoDAO,
            MetricsFactory metricsFactory,
            boolean isMultiStreamMode,
            long streamInfoBackfillIntervalMillis,
            StreamInfoMode streamInfoMode);
}
