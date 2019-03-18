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
package com.amazonaws.services.kinesis.multilang.messages;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;

/**
 * A message to indicate to the client's process that it should shutdown and then terminate.
 */
public class ShutdownMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "shutdown";

    /**
     * The reason for shutdown, e.g. TERMINATE or ZOMBIE
     */
    private String reason;

    /**
     * Default constructor.
     */
    public ShutdownMessage() {
    }

    /**
     * Convenience constructor.
     * 
     * @param reason The reason.
     */
    public ShutdownMessage(ShutdownReason reason) {
        if (reason == null) {
            this.setReason(null);
        } else {
            this.setReason(String.valueOf(reason));
        }
    }

    /**
     * @return reason The reason.
     */
    public String getReason() {
        return reason;
    }

    /**
     * @param reason The reason.
     */
    public void setReason(String reason) {
        this.reason = reason;
    }
}
