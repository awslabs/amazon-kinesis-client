/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.common;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ThreadExceptionReporter implements Thread.UncaughtExceptionHandler {

    private final String threadSource;
    private final String lastDitchMessage;

    public ThreadExceptionReporter(String threadSource) {
        this.threadSource = threadSource;
        this.lastDitchMessage = "[" + threadSource + "] Thread died, and unable to log normally";
    }


    @Override
    public void uncaughtException(Thread t, Throwable e) {

        try {
            log.error("[{}] Thread {} (id-{}) died unexpectedly due to {}", threadSource, t.getName(), t.getId(), e.getMessage(), e);
        } catch (Throwable logThrowable) {
            //
            // If the system is very unhealthy it's possible we won't be able create a log message so we attempt to log the last ditch message
            //
            attemptToLogLastDitchMessage();
        }

    }

    private void attemptToLogLastDitchMessage() {
        try {
            log.error(lastDitchMessage);
        } catch (Throwable t) {
            //
            // Logging failed for some reason attempt to write to standard error
            //
            attemptToWriteStdErrLastDitchMessage();
        }
    }

    private void attemptToWriteStdErrLastDitchMessage() {
        try {
            System.err.print(lastDitchMessage);
            System.err.print("\n");
        } catch (Throwable t) {
            //
            // All options are now exhausted allow the message to be lost.
            //
        }
    }
}
