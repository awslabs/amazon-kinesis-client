/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.multilang.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import software.amazon.kinesis.lifecycle.ShutdownReason;

/**
 * A message to indicate to the client's process that it should shutdown and then terminate.
 */
@NoArgsConstructor
@Getter
@Setter
public class ShutdownMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "shutdown";

    /**
     * The reason for shutdown, e.g. SHARD_END or LEASE_LOST
     */
    private String reason;

    public ShutdownMessage(final ShutdownReason reason) {
        if (reason != null) {
            this.reason = String.valueOf(reason);
        }
    }
}
