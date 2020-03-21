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

package software.amazon.kinesis.common;

import org.slf4j.Logger;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

import java.time.Duration;
import java.time.Instant;

import static software.amazon.kinesis.lifecycle.ShardConsumer.MAX_TIME_BETWEEN_REQUEST_RESPONSE;

@KinesisClientInternalApi
public class DiagnosticUtils {

    /**
     * Util for RecordPublisher to measure the event delivery latency of the executor service and take appropriate action.
     * @param resourceIdentifier of the shard that is having delayed delivery
     * @param enqueueTimestamp of the event submitted to the executor service
     * @param log Slf4j Logger from RecordPublisher to log the events
     */
    public static void takeDelayedDeliveryActionIfRequired(String resourceIdentifier, Instant enqueueTimestamp, Logger log) {
        final long durationBetweenEnqueueAndAckInMillis = Duration
                .between(enqueueTimestamp, Instant.now()).toMillis();
        if (durationBetweenEnqueueAndAckInMillis > MAX_TIME_BETWEEN_REQUEST_RESPONSE / 3) {
            // The above condition logs the warn msg if the delivery time exceeds 11 seconds.
            log.warn(
                    "{}: Record delivery time to shard consumer is high at {} millis. Check the ExecutorStateEvent logs"
                            + " to see the state of the executor service. Also check if the RecordProcessor's processing "
                            + "time is high. ",
                    resourceIdentifier, durationBetweenEnqueueAndAckInMillis);
        } else if (log.isDebugEnabled()) {
            log.debug("{}: Record delivery time to shard consumer is {} millis", resourceIdentifier,
                    durationBetweenEnqueueAndAckInMillis);
        }
    }
}
