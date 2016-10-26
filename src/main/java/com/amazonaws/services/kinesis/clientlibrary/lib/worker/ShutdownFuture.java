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
            throws InterruptedException, ExecutionException, TimeoutException {

        final long startNanos = System.nanoTime();

        //
        // Awaiting for all ShardConsumer/RecordProcessors to be notified that a shutdown has been requested.
        // There is the possibility of a race condition where a lease is terminated after the shutdown request
        // notification is started, but before the ShardConsumer is sent the notification. In this case the
        // ShardConsumer would start the lease loss shutdown, and may never call the notification methods.
        //
        if (!notificationCompleteLatch.await(timeout, unit)) {
            long awaitingNotification = notificationCompleteLatch.getCount();
            long awaitingFinalShutdown = shutdownCompleteLatch.getCount();
            log.info("Awaiting " + awaitingNotification + " record processors to complete shutdown notification, and "
                    + awaitingFinalShutdown + " awaiting final shutdown");
            if (awaitingFinalShutdown != 0) {
                //
                // The number of record processor awaiting final shutdown should be a superset of the those awaiting
                // notification
                //
                return checkWorkerShutdownMiss(awaitingFinalShutdown);
            }
        }

        long remaining = remainingTimeout(timeout, unit, startNanos);
        throwTimeoutMessageIfExceeded(remaining, "Notification hasn't completed within timeout time.");

        //
        // Once all record processors have been notified of the shutdown it is safe to allow the worker to
        // start its shutdown behavior. Once shutdown starts it will stop renewer, and drop any remaining leases.
        //
        worker.shutdown();
        remaining = remainingTimeout(timeout, unit, startNanos);
        throwTimeoutMessageIfExceeded(remaining, "Shutdown hasn't completed within timeout time.");

        //
        // Want to wait for all the remaining ShardConsumers/RecordProcessor's to complete their final shutdown
        // processing. This should really be a no-op since as part of the notification completion the lease for
        // ShardConsumer is terminated.
        //
        if (!shutdownCompleteLatch.await(remaining, TimeUnit.NANOSECONDS)) {
            long outstanding = shutdownCompleteLatch.getCount();
            log.info("Awaiting " + outstanding + " record processors to complete final shutdown");

            return checkWorkerShutdownMiss(outstanding);
        }
        return 0;
    }

    private long remainingTimeout(long timeout, TimeUnit unit, long startNanos) {
        long checkNanos = System.nanoTime() - startNanos;
        return unit.toNanos(timeout) - checkNanos;
    }

    private void throwTimeoutMessageIfExceeded(long remainingNanos, String message) throws TimeoutException {
        if (remainingNanos <= 0) {
            throw new TimeoutException(message);
        }
    }

    /**
     * This checks to see if the worker has already hit it's shutdown target, while there is outstanding record
     * processors. This maybe a little racy due to when the value of outstanding is retrieved. In general though the
     * latch should be decremented before the shutdown completion.
     *
     * @param outstanding
     *            the number of record processor still awaiting shutdown.
     * @return the number of record processors awaiting shutdown, or 0 if the worker believes it's shutdown already.
     */
    private long checkWorkerShutdownMiss(long outstanding) {
        if (isWorkerShutdownComplete()) {
            if (outstanding != 0) {
                log.info("Shutdown completed, but shutdownCompleteLatch still had outstanding " + outstanding
                        + " with a current value of " + shutdownCompleteLatch.getCount() + ". shutdownComplete: "
                        + worker.isShutdownComplete() + " -- Consumer Map: "
                        + worker.getShardInfoShardConsumerMap().size());
            }
            return 0;
        }
        return outstanding;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        boolean complete = false;
        do {
            try {
                long outstanding = outstandingRecordProcessors(1, TimeUnit.SECONDS);
                complete = outstanding == 0;
                log.info("Awaiting " + outstanding + " consumer(s) to finish shutdown.");
            } catch (TimeoutException te) {
                log.info("Timeout while waiting for completion: " + te.getMessage());
            }

        } while(!complete);
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
