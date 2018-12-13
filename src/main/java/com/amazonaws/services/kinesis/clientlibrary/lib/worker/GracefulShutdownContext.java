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

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

class GracefulShutdownContext {
    private final CountDownLatch shutdownCompleteLatch;
    private final CountDownLatch notificationCompleteLatch;
    private final Worker worker;

    static GracefulShutdownContext SHUTDOWN_ALREADY_COMPLETED = new GracefulShutdownContext(null, null, null);

    public GracefulShutdownContext(CountDownLatch shutdownCompleteLatch, CountDownLatch notificationCompleteLatch, Worker worker) {
        this.shutdownCompleteLatch = shutdownCompleteLatch;
        this.notificationCompleteLatch = notificationCompleteLatch;
        this.worker = worker;
        Objects.requireNonNull(shutdownCompleteLatch);
        Objects.requireNonNull(notificationCompleteLatch);
        Objects.requireNonNull(worker);
    }

    boolean isShutdownAlreadyCompleted() {
        return shutdownCompleteLatch == null && notificationCompleteLatch == null && worker == null;
    }

    public CountDownLatch getShutdownCompleteLatch() {
        return shutdownCompleteLatch;
    }

    public CountDownLatch getNotificationCompleteLatch() {
        return notificationCompleteLatch;
    }

    public Worker getWorker() {
        return worker;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GracefulShutdownContext that = (GracefulShutdownContext) o;
        return Objects.equals(shutdownCompleteLatch, that.shutdownCompleteLatch) &&
                Objects.equals(notificationCompleteLatch, that.notificationCompleteLatch) &&
                Objects.equals(worker, that.worker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shutdownCompleteLatch, notificationCompleteLatch, worker);
    }

    @Override
    public String toString() {
        return "GracefulShutdownContext{" +
                "shutdownCompleteLatch=" + shutdownCompleteLatch +
                ", notificationCompleteLatch=" + notificationCompleteLatch +
                ", worker=" + worker +
                '}';
    }
}
