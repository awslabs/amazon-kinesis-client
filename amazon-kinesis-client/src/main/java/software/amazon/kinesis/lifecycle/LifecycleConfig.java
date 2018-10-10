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
import software.amazon.kinesis.retrieval.AggregatorUtil;

/**
 * Used by the KCL to configure the lifecycle.
 */
@Data
@Accessors(fluent = true)
public class LifecycleConfig {
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
}
