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
package software.amazon.kinesis.multilang.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
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
