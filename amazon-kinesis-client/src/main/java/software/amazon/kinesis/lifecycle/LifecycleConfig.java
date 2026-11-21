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
     * Gives up on a {@link ShardConsumer} whose task has been running for longer than the set time.
     *
     * <p>A record processor that never returns leaves its shard consumer wedged: the subscription restart
     * timer is disarmed while a task is in flight, no exception is raised, and the consumer can never reach a
     * shutdown state, so it is never replaced. The shard stops making progress for the lifetime of the
     * process while its lease keeps being renewed.
     *
     * <p>When set, the scheduler cancels the subscription of such a consumer, drops it, and releases the
     * lease so that a fresh consumer picks the shard up and resumes from the last checkpoint. The thread
     * that is stuck inside the record processor is left behind and is not interrupted.
     *
     * <p>Default value: {@link Optional#empty()}, meaning a wedged consumer is never given up on.
     */
    private Optional<Long> abandonTaskAfterMillis = Optional.empty();

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
