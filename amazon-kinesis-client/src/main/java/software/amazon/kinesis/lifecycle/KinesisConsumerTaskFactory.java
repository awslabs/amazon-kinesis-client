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

package software.amazon.kinesis.lifecycle;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.ThrottlingReporter;

@KinesisClientInternalApi
public class KinesisConsumerTaskFactory implements ConsumerTaskFactory {

    @Override
    public ConsumerTask createShutdownTask(
            ShardConsumerArgument argument, ShardConsumer consumer, ProcessRecordsInput input) {
        return new ShutdownTask(
                argument.shardInfo(),
                argument.shardDetector(),
                argument.shardRecordProcessor(),
                argument.recordProcessorCheckpointer(),
                consumer.shutdownReason(),
                argument.initialPositionInStream(),
                argument.cleanupLeasesOfCompletedShards(),
                argument.ignoreUnexpectedChildShards(),
                argument.leaseCoordinator(),
                argument.taskBackoffTimeMillis(),
                argument.recordsPublisher(),
                argument.hierarchicalShardSyncer(),
                argument.metricsFactory(),
                input == null ? null : input.childShards(),
                argument.streamIdentifier(),
                argument.leaseCleanupManager());
    }

    @Override
    public ConsumerTask createProcessTask(ShardConsumerArgument argument, ProcessRecordsInput processRecordsInput) {
        ThrottlingReporter throttlingReporter =
                new ThrottlingReporter(5, argument.shardInfo().shardId());
        return new ProcessTask(
                argument.shardInfo(),
                argument.shardRecordProcessor(),
                argument.recordProcessorCheckpointer(),
                argument.taskBackoffTimeMillis(),
                argument.skipShardSyncAtWorkerInitializationIfLeasesExist(),
                argument.shardDetector(),
                throttlingReporter,
                processRecordsInput,
                argument.shouldCallProcessRecordsEvenForEmptyRecordList(),
                argument.idleTimeInMilliseconds(),
                argument.aggregatorUtil(),
                argument.metricsFactory(),
                argument.schemaRegistryDecoder(),
                argument.leaseCoordinator().leaseStatsRecorder());
    }

    @Override
    public ConsumerTask createInitializeTask(ShardConsumerArgument argument) {
        return new InitializeTask(
                argument.shardInfo(),
                argument.shardRecordProcessor(),
                argument.checkpoint(),
                argument.recordProcessorCheckpointer(),
                argument.initialPositionInStream(),
                argument.recordsPublisher(),
                argument.taskBackoffTimeMillis(),
                argument.metricsFactory());
    }

    @Override
    public ConsumerTask createBlockOnParentTask(ShardConsumerArgument argument) {
        return new BlockOnParentShardTask(
                argument.shardInfo(),
                argument.leaseCoordinator().leaseRefresher(),
                argument.parentShardPollIntervalMillis());
    }

    @Override
    public ConsumerTask createShutdownNotificationTask(ShardConsumerArgument argument, ShardConsumer consumer) {
        return new ShutdownNotificationTask(
                argument.shardRecordProcessor(),
                argument.recordProcessorCheckpointer(),
                consumer.shutdownNotification(),
                argument.shardInfo(),
                consumer.shardConsumerArgument().leaseCoordinator());
    }
}
