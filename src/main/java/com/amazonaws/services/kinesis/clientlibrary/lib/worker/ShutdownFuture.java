package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Used as a response from the {@link Worker#requestShutdown()} to allow callers to wait until shutdown is complete.
 */
class ShutdownFuture implements Future<Void> {

    private static final Log log = LogFactory.getLog(ShutdownFuture.class);

    private final CountDownLatch shutdownCompleteLatch;
    private final CountDownLatch notificationCompleteLatch;
    private final Worker worker;

    private boolean workerShutdownCalled = false;

    ShutdownFuture(CountDownLatch shutdownCompleteLatch, CountDownLatch notificationCompleteLatch, Worker worker) {
        this.shutdownCompleteLatch = shutdownCompleteLatch;
        this.notificationCompleteLatch = notificationCompleteLatch;
        this.worker = worker;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Cannot cancel a shutdown process");
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return isWorkerShutdownComplete();
    }

    private boolean isWorkerShutdownComplete() {
        return worker.isShutdownComplete() || worker.getShardInfoShardConsumerMap().isEmpty();
    }

    private long outstandingRecordProcessors(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException {
        //
        // Awaiting for all ShardConsumer/RecordProcessors to be notified that a shutdown has been requested.
        //
        if (!notificationCompleteLatch.await(timeout, unit)) {
            long awaitingNotification = notificationCompleteLatch.getCount();
            log.info("Awaiting " + awaitingNotification + " record processors to complete initial shutdown");
            long awaitingFinalShutdown = shutdownCompleteLatch.getCount();
            if (awaitingFinalShutdown != 0) {
                return awaitingFinalShutdown;
            }
        }
        //
        // Once all record processors have been notified of the shutdown it is safe to allow the worker to
        // start its shutdown behavior. Once shutdown starts it will stop renewer, and drop any remaining leases.
        //
        if (!workerShutdownCalled) {
            //
            // Unfortunately Worker#shutdown() doesn't appear to be idempotent.
            //
            worker.shutdown();
        }
        //
        // Want to wait for all the remaining ShardConsumers/RecordProcessor's to complete their final shutdown
        // processing. This should really be a no-op since as part of the notification completion the lease for
        // ShardConsumer is terminated.
        //
        if (!shutdownCompleteLatch.await(timeout, unit)) {
            long outstanding = shutdownCompleteLatch.getCount();
            log.info("Awaiting " + outstanding + " record processors to complete final shutdown");
            if (isWorkerShutdownComplete()) {
                if (outstanding != 0) {
                    log.warn("Shutdown completed, but shutdownCompleteLatch still had outstanding " + outstanding
                            + " with a current value of " + shutdownCompleteLatch.getCount()
                            + ". shutdownComplete: " + worker.isShutdownComplete() + " -- Consumer Map: "
                            + worker.getShardInfoShardConsumerMap().size());
                }
                return 0;
            }
            return outstanding;
        }
        return 0;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        long outstanding;
        do {
            outstanding = outstandingRecordProcessors(1, TimeUnit.SECONDS);
            log.info("Awaiting " + outstanding + " consumer(s) to finish shutdown.");
        } while(outstanding != 0);
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long outstanding = outstandingRecordProcessors(timeout, unit);
        if (outstanding != 0) {
            throw new TimeoutException("Awaiting " + outstanding + " record processors to shutdown.");
        }
        return null;
    }
}
