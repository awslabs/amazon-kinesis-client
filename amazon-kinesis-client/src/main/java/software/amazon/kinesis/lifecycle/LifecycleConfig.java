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

package software.amazon.kinesis.lifecycle;

import java.util.Optional;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Used by the KCL to configure the lifecycle.
 */
@Data
@Accessors(fluent = true)
public class LifecycleConfig {
    /**
     * The amount of milliseconds to wait before graceful shutdown forcefully terminates.
     *
     * <p>Default value: 5000L</p>
     */
    private long shutdownGraceMillis = 5000L;

    /**
     * Logs warn message if as task is held in  a task for more than the set time.
     *
     * <p>Default value: {@link Optional#empty()}</p>
     */
    private Optional<Long> logWarningForTaskAfterMillis = Optional.empty();

    /**
     * Backoff time in milliseconds for Amazon Kinesis Client Library tasks (in the event of failures).
     *
     * <p>Default value: 500L</p>
     */
    private long taskBackoffTimeMillis = 500L;
}
