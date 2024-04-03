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
package software.amazon.kinesis.coordinator;


import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class GracefulShutdownCoordinator {

    /**
     * arbitrary wait time for worker's finalShutdown
     */
    private static final long FINAL_SHUTDOWN_WAIT_TIME_SECONDS = 60L;

    CompletableFuture<Boolean> startGracefulShutdown(Callable<Boolean> shutdownCallable) {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try { cf.complete(shutdownCallable.call()); }
            catch(Throwable ex) { cf.completeExceptionally(ex); }
        });
        return cf;
    }

    Callable<Boolean> createGracefulShutdownCallable(Callable<GracefulShutdownContext> startWorkerShutdown) {
        return new GracefulShutdownCallable(startWorkerShutdown);
    }

    @Slf4j
    static class GracefulShutdownCallable implements Callable<Boolean> {
        private final Callable<GracefulShutdownContext> startWorkerShutdown;

        GracefulShutdownCallable(Callable<GracefulShutdownContext> startWorkerShutdown) {
            this.startWorkerShutdown = startWorkerShutdown;
        }

        private boolean isWorkerShutdownComplete(GracefulShutdownContext context) {
            return context.scheduler().shutdownComplete() || context.scheduler().shardInfoShardConsumerMap().isEmpty();
        }

        private String awaitingLogMessage(GracefulShutdownContext context) {
            long awaitingNotification = context.notificationCompleteLatch().getCount();
            long awaitingFinalShutdown = context.shutdownCompleteLatch().getCount();

            return String.format(
                    "Waiting for %d record process to complete shutdown notification, and %d record processor to complete final shutdown ",
                    awaitingNotification, awaitingFinalShutdown);
        }

        private String awaitingFinalShutdownMessage(GracefulShutdownContext context) {
            long outstanding = context.shutdownCompleteLatch().getCount();
            return String.format("Waiting for %d record processors to complete final shutdown", outstanding);
        }

        /**
         * used to wait for the worker's final shutdown to complete before returning the future for graceful shutdown
         * @return true if the final shutdown is successful, false otherwise.
         */
        private boolean waitForFinalShutdown(GracefulShutdownContext context) throws InterruptedException {
            return context.finalShutdownLatch().await(FINAL_SHUTDOWN_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        }

        private boolean waitForRecordProcessors(GracefulShutdownContext context) {
            if (context.isRecordProcessorShutdownComplete()) {
                return true;
            }

            //
            // Awaiting for all ShardConsumer/RecordProcessors to be notified that a shutdown has been requested.
            // There is the possibility of a race condition where a lease is terminated after the shutdown request
            // notification is started, but before the ShardConsumer is sent the notification. In this case the
            // ShardConsumer would start the lease loss shutdown, and may never call the notification methods.
            //
            try {
                while (!context.notificationCompleteLatch().await(1, TimeUnit.SECONDS)) {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    log.info(awaitingLogMessage(context));
                    if (workerShutdownWithRemaining(context.shutdownCompleteLatch().getCount(), context)) {
                        return false;
                    }
                }
            } catch (InterruptedException ie) {
                log.warn("Interrupted while waiting for notification complete, terminating shutdown. {}",
                        awaitingLogMessage(context));
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
            context.scheduler().shutdown();

            if (Thread.interrupted()) {
                log.warn("Interrupted after worker shutdown, terminating shutdown");
                return false;
            }

            //
            // Want to wait for all the remaining ShardConsumers/ShardRecordProcessor's to complete their final shutdown
            // processing. This should really be a no-op since as part of the notification completion the lease for
            // ShardConsumer is terminated.
            //
            try {
                while (!context.shutdownCompleteLatch().await(1, TimeUnit.SECONDS)) {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    log.info(awaitingFinalShutdownMessage(context));
                    if (workerShutdownWithRemaining(context.shutdownCompleteLatch().getCount(), context)) {
                        return false;
                    }
                }
            } catch (InterruptedException ie) {
                log.warn("Interrupted while waiting for shutdown completion, terminating shutdown. {}",
                        awaitingFinalShutdownMessage(context));
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
                    log.info("Shutdown completed, but shutdownCompleteLatch still had outstanding {} with a current"
                            + " value of {}. shutdownComplete: {} -- Consumer Map: {}", outstanding,
                            context.shutdownCompleteLatch().getCount(), context.scheduler().shutdownComplete(),
                            context.scheduler().shardInfoShardConsumerMap().size());
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                final GracefulShutdownContext context = startWorkerShutdown.call();
                return waitForRecordProcessors(context) && waitForFinalShutdown(context);
            } catch (Exception ex) {
                log.warn("Caught exception while requesting initial worker shutdown.", ex);
                throw ex;
            }
        }
    }
}
