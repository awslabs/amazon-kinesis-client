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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class GracefulShutdownCoordinator {

    Future<Boolean> startGracefulShutdown(Callable<Boolean> shutdownCallable) {
        FutureTask<Boolean> task = new FutureTask<>(shutdownCallable);
        Thread shutdownThread = new Thread(task, "RequestedShutdownThread");
        shutdownThread.start();
        return task;

    }

    Callable<Boolean> createGracefulShutdownCallable(Callable<GracefulShutdownContext> startWorkerShutdown) {
        return new GracefulShutdownCallable(startWorkerShutdown);
    }

    static class GracefulShutdownCallable implements Callable<Boolean> {

        private static final Log log = LogFactory.getLog(GracefulShutdownCallable.class);

        private final Callable<GracefulShutdownContext> startWorkerShutdown;

        GracefulShutdownCallable(Callable<GracefulShutdownContext> startWorkerShutdown) {
            this.startWorkerShutdown = startWorkerShutdown;
        }

        private boolean isWorkerShutdownComplete(GracefulShutdownContext context) {
            return context.getWorker().isShutdownComplete() || context.getWorker().getShardInfoShardConsumerMap().isEmpty();
        }

        private String awaitingLogMessage(GracefulShutdownContext context) {
            long awaitingNotification = context.getNotificationCompleteLatch().getCount();
            long awaitingFinalShutdown = context.getShutdownCompleteLatch().getCount();

            return String.format(
                    "Waiting for %d record process to complete shutdown notification, and %d record processor to complete final shutdown ",
                    awaitingNotification, awaitingFinalShutdown);
        }

        private String awaitingFinalShutdownMessage(GracefulShutdownContext context) {
            long outstanding = context.getShutdownCompleteLatch().getCount();
            return String.format("Waiting for %d record processors to complete final shutdown", outstanding);
        }

        private boolean waitForRecordProcessors(GracefulShutdownContext context) {

            //
            // Awaiting for all ShardConsumer/RecordProcessors to be notified that a shutdown has been requested.
            // There is the possibility of a race condition where a lease is terminated after the shutdown request
            // notification is started, but before the ShardConsumer is sent the notification. In this case the
            // ShardConsumer would start the lease loss shutdown, and may never call the notification methods.
            //
            try {
                while (!context.getNotificationCompleteLatch().await(1, TimeUnit.SECONDS)) {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    log.info(awaitingLogMessage(context));
                    if (workerShutdownWithRemaining(context.getShutdownCompleteLatch().getCount(), context)) {
                        return false;
                    }
                }
            } catch (InterruptedException ie) {
                log.warn("Interrupted while waiting for notification complete, terminating shutdown.  "
                        + awaitingLogMessage(context));
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
            context.getWorker().shutdown();

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
                while (!context.getShutdownCompleteLatch().await(1, TimeUnit.SECONDS)) {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    log.info(awaitingFinalShutdownMessage(context));
                    if (workerShutdownWithRemaining(context.getShutdownCompleteLatch().getCount(), context)) {
                        return false;
                    }
                }
            } catch (InterruptedException ie) {
                log.warn("Interrupted while waiting for shutdown completion, terminating shutdown. "
                        + awaitingFinalShutdownMessage(context));
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
        private boolean workerShutdownWithRemaining(long outstanding, GracefulShutdownContext context) {
            if (isWorkerShutdownComplete(context)) {
                if (outstanding != 0) {
                    log.info("Shutdown completed, but shutdownCompleteLatch still had outstanding " + outstanding
                            + " with a current value of " + context.getShutdownCompleteLatch().getCount() + ". shutdownComplete: "
                            + context.getWorker().isShutdownComplete() + " -- Consumer Map: "
                            + context.getWorker().getShardInfoShardConsumerMap().size());
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean call() throws Exception {
            GracefulShutdownContext context;
            try {
                context = startWorkerShutdown.call();
            } catch (Exception ex) {
                log.warn("Caught exception while requesting initial worker shutdown.", ex);
                throw ex;
            }
            return context.isShutdownAlreadyCompleted() || waitForRecordProcessors(context);
        }
    }
}
