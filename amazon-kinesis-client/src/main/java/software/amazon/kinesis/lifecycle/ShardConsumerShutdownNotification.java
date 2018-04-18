/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;

import java.util.concurrent.CountDownLatch;

import software.amazon.kinesis.processor.ShutdownNotificationAware;
import software.amazon.kinesis.leases.KinesisClientLease;
import software.amazon.kinesis.leases.LeaseCoordinator;

/**
 * Contains callbacks for completion of stages in a requested record processor shutdown.
 *
 */
public class ShardConsumerShutdownNotification implements ShutdownNotification {

    private final LeaseCoordinator<KinesisClientLease> leaseCoordinator;
    private final KinesisClientLease lease;
    private final CountDownLatch shutdownCompleteLatch;
    private final CountDownLatch notificationCompleteLatch;

    private boolean notificationComplete = false;
    private boolean allNotificationCompleted = false;

    /**
     * Creates a new shutdown request object.
     * 
     * @param leaseCoordinator
     *            the lease coordinator used to drop leases from once the initial shutdown request is completed.
     * @param lease
     *            the lease that this shutdown request will free once initial shutdown is complete
     * @param notificationCompleteLatch
     *            used to inform the caller once the
     *            {@link ShutdownNotificationAware} object has been
     *            notified of the shutdown request.
     * @param shutdownCompleteLatch
     *            used to inform the caller once the record processor is fully shutdown
     */
    public ShardConsumerShutdownNotification(LeaseCoordinator<KinesisClientLease> leaseCoordinator, KinesisClientLease lease,
                                             CountDownLatch notificationCompleteLatch, CountDownLatch shutdownCompleteLatch) {
        this.leaseCoordinator = leaseCoordinator;
        this.lease = lease;
        this.notificationCompleteLatch = notificationCompleteLatch;
        this.shutdownCompleteLatch = shutdownCompleteLatch;
    }

    @Override
    public void shutdownNotificationComplete() {
        if (notificationComplete) {
            return;
        }
        //
        // Once the notification has been completed, the lease needs to dropped to allow the worker to complete
        // shutdown of the record processor.
        //
        leaseCoordinator.dropLease(lease);
        notificationCompleteLatch.countDown();
        notificationComplete = true;
    }

    @Override
    public void shutdownComplete() {
        if (allNotificationCompleted) {
            return;
        }
        if (!notificationComplete) {
            notificationCompleteLatch.countDown();
        }
        shutdownCompleteLatch.countDown();
        allNotificationCompleted = true;
    }

}
