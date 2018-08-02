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
package software.amazon.kinesis.lifecycle;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;

/**
 * Notifies record processor of incoming shutdown request, and gives them a chance to checkpoint.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@Slf4j
@KinesisClientInternalApi
public class ShutdownNotificationTask implements ConsumerTask {
    private final ShardRecordProcessor shardRecordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ShutdownNotification shutdownNotification;
//    TODO: remove if not used
    private final ShardInfo shardInfo;

    @Override
    public TaskResult call() {
        try {
            try {
                shardRecordProcessor.shutdownRequested(ShutdownRequestedInput.builder().checkpointer(recordProcessorCheckpointer).build());
            } catch (Exception ex) {
                return new TaskResult(ex);
            }

            return new TaskResult(null);
        } finally {
            shutdownNotification.shutdownNotificationComplete();
        }
    }

    @Override
    public TaskType taskType() {
        return TaskType.SHUTDOWN_NOTIFICATION;
    }

}
