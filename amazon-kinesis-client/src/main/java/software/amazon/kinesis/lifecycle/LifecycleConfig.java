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

package software.amazon.kinesis.lifecycle;

import java.util.Optional;

import lombok.Data;
import lombok.experimental.Accessors;
import software.amazon.kinesis.retrieval.AggregatorUtil;

/**
 * Used by the KCL to configure the lifecycle.
 */
@Data
@Accessors(fluent = true)
public class LifecycleConfig {
    public static final int DEFAULT_READ_TIMEOUTS_TO_IGNORE = 0;
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

    /**
     * AggregatorUtil is responsible for deaggregating KPL records.
     */
    private AggregatorUtil aggregatorUtil = new AggregatorUtil();

    /**
     * TaskExecutionListener to be used to handle events during task execution lifecycle for a shard.
     *
     * <p>Default value: {@link NoOpTaskExecutionListener}</p>
     */
    private TaskExecutionListener taskExecutionListener = new NoOpTaskExecutionListener();

    /**
     * Number of consecutive ReadTimeouts to ignore before logging warning messages.
     * If you find yourself seeing frequent ReadTimeout, you should also consider increasing your timeout according to
     * your expected processing time.
     *
     * <p>Default value: 0</p>
     */
    private int readTimeoutsToIgnoreBeforeWarning = DEFAULT_READ_TIMEOUTS_TO_IGNORE;
}
