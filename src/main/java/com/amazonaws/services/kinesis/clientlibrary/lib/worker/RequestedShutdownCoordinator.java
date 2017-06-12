package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class RequestedShutdownCoordinator {

    static Future<Boolean> startRequestedShutdown(Callable<Boolean> shutdownCallable) {
        FutureTask<Boolean> task = new FutureTask<>(shutdownCallable);
        Thread shutdownThread = new Thread(task, "RequestedShutdownThread");
        shutdownThread.start();
        return task;

    }

    static Callable<Boolean> createRequestedShutdownCallable(CountDownLatch shutdownCompleteLatch,
            CountDownLatch notificationCompleteLatch, Worker worker) {
        return new RequestedShutdownCallable(shutdownCompleteLatch, notificationCompleteLatch, worker);
    }

    static class RequestedShutdownCallable implements Callable<Boolean> {

        private static final Log log = LogFactory.getLog(RequestedShutdownCallable.class);

        private final CountDownLatch shutdownCompleteLatch;
        private final CountDownLatch notificationCompleteLatch;
        private final Worker worker;

        RequestedShutdownCallable(CountDownLatch shutdownCompleteLatch, CountDownLatch notificationCompleteLatch,
                Worker worker) {
            this.shutdownCompleteLatch = shutdownCompleteLatch;
            this.notificationCompleteLatch = notificationCompleteLatch;
            this.worker = worker;
        }

        private boolean isWorkerShutdownComplete() {
            return worker.isShutdownComplete() || worker.getShardInfoShardConsumerMap().isEmpty();
        }

        private String awaitingLogMessage() {
            long awaitingNotification = notificationCompleteLatch.getCount();
            long awaitingFinalShutdown = shutdownCompleteLatch.getCount();

            return String.format(
                    "Waiting for %d record process to complete shutdown notification, and %d record processor to complete final shutdown ",
                    awaitingNotification, awaitingFinalShutdown);
        }

        private String awaitingFinalShutdownMessage() {
            long outstanding = shutdownCompleteLatch.getCount();
            return String.format("Waiting for %d record processors to complete final shutdown", outstanding);
        }

        private boolean waitForRecordProcessors() {

            //
            // Awaiting for all ShardConsumer/RecordProcessors to be notified that a shutdown has been requested.
            // There is the possibility of a race condition where a lease is terminated after the shutdown request
            // notification is started, but before the ShardConsumer is sent the notification. In this case the
            // ShardConsumer would start the lease loss shutdown, and may never call the notification methods.
            //
            try {
                while (!notificationCompleteLatch.await(1, TimeUnit.SECONDS)) {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    log.info(awaitingLogMessage());
                    if (workerShutdownWithRemaining(shutdownCompleteLatch.getCount())) {
                        return false;
                    }
                }
            } catch (InterruptedException ie) {
                log.warn("Interrupted while waiting for notification complete, terminating shutdown.  "
                        + awaitingLogMessage());
                return false;
            }

            if (Thread.interrupted()) {
                log.warn("Interrupted before worker shutdown, terminating shutdown");
                return false;
            }

            //
            // Once all record processors have been notified of the shutdown it is safe to allow the worker to
            // start its shutdown behavior. Once shutdown starts it will stop renewer, and drop any remaining leases.
            //
            worker.shutdown();

            if (Thread.interrupted()) {
                log.warn("Interrupted after worker shutdown, terminating shutdown");
                return false;
            }

            //
            // Want to wait for all the remaining ShardConsumers/RecordProcessor's to complete their final shutdown
            // processing. This should really be a no-op since as part of the notification completion the lease for
            // ShardConsumer is terminated.
            //
            try {
                while (!shutdownCompleteLatch.await(1, TimeUnit.SECONDS)) {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    log.info(awaitingFinalShutdownMessage());
                    if (workerShutdownWithRemaining(shutdownCompleteLatch.getCount())) {
                        return false;
                    }
                }
            } catch (InterruptedException ie) {
                log.warn("Interrupted while waiting for shutdown completion, terminating shutdown. "
                        + awaitingFinalShutdownMessage());
                return false;
            }
            return true;
        }

        /**
         * This checks to see if the worker has already hit it's shutdown target, while there is outstanding record
         * processors. This maybe a little racy due to when the value of outstanding is retrieved. In general though the
         * latch should be decremented before the shutdown completion.
         *
         * @param outstanding
         *            the number of record processor still awaiting shutdown.
         */
        private boolean workerShutdownWithRemaining(long outstanding) {
            if (isWorkerShutdownComplete()) {
                if (outstanding != 0) {
                    log.info("Shutdown completed, but shutdownCompleteLatch still had outstanding " + outstanding
                            + " with a current value of " + shutdownCompleteLatch.getCount() + ". shutdownComplete: "
                            + worker.isShutdownComplete() + " -- Consumer Map: "
                            + worker.getShardInfoShardConsumerMap().size());
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean call() throws Exception {
            return waitForRecordProcessors();
        }
    }
}
