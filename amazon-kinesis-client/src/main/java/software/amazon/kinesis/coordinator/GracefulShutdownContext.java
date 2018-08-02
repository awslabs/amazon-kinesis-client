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
package software.amazon.kinesis.coordinator;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.concurrent.CountDownLatch;

@Data
@Accessors(fluent = true)
class GracefulShutdownContext {
    private final CountDownLatch shutdownCompleteLatch;
    private final CountDownLatch notificationCompleteLatch;
    private final Scheduler scheduler;

    static GracefulShutdownContext SHUTDOWN_ALREADY_COMPLETED = new GracefulShutdownContext(null, null, null);

    boolean isShutdownAlreadyCompleted() {
        return shutdownCompleteLatch == null && notificationCompleteLatch == null && scheduler == null;
    }

}
