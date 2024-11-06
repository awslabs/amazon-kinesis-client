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

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
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
    private final ShardInfo shardInfo;
    private final LeaseCoordinator leaseCoordinator;

    @Override
    public TaskResult call() {
        final String leaseKey = ShardInfo.getLeaseKey(shardInfo);
        final Lease currentShardLease = leaseCoordinator.getCurrentlyHeldLease(leaseKey);
        try {
            try {
                shardRecordProcessor.shutdownRequested(ShutdownRequestedInput.builder()
                        .checkpointer(recordProcessorCheckpointer)
                        .build());
                attemptLeaseTransfer(currentShardLease);
            } catch (Exception ex) {
                return new TaskResult(ex);
            }
            return new TaskResult(null);
        } finally {
            if (shutdownNotification != null) {
                shutdownNotification.shutdownNotificationComplete();
            } else {
                // shutdownNotification is null if this is a shard level graceful shutdown instead of a worker level
                // one. We need to drop lease like what's done in the shutdownNotificationComplete so we can
                // transition to next state.
                leaseCoordinator.dropLease(currentShardLease);
            }
        }
    }

    private void attemptLeaseTransfer(Lease lease)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        if (lease != null && lease.shutdownRequested()) {
            if (leaseCoordinator.workerIdentifier().equals(lease.checkpointOwner())) {
                leaseCoordinator.leaseRefresher().assignLease(lease, lease.leaseOwner());
            }
        }
    }

    @Override
    public TaskType taskType() {
        return TaskType.SHUTDOWN_NOTIFICATION;
    }
}
