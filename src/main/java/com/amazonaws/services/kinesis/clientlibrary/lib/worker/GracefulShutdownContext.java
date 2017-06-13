package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import lombok.Data;

import java.util.concurrent.CountDownLatch;

@Data
class GracefulShutdownContext {
    private final CountDownLatch shutdownCompleteLatch;
    private final CountDownLatch notificationCompleteLatch;
    private final Worker worker;

    static GracefulShutdownContext SHUTDOWN_ALREADY_COMPLETED = new GracefulShutdownContext(null, null, null);

    boolean isShutdownAlreadyCompleted() {
        return shutdownCompleteLatch == null && notificationCompleteLatch == null && worker == null;
    }

}
