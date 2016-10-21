package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.CountDownLatch;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.LeaseCoordinator;

/**
 * Contains callbacks for completion of stages in a requested record processor shutdown.
 *
 */
class ShardConsumerShutdownNotification implements ShutdownNotification {

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
     *            {@link IShutdownNotificationAware} object has been
     *            notified of the shutdown request.
     * @param shutdownCompleteLatch
     *            used to inform the caller once the record processor is fully shutdown
     */
    ShardConsumerShutdownNotification(LeaseCoordinator<KinesisClientLease> leaseCoordinator, KinesisClientLease lease,
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
