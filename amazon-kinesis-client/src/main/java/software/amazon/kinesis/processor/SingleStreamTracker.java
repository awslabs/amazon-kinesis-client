/*
 * Copyright 2023 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.processor;

import java.util.Collections;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import software.amazon.awssdk.arns.Arn;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;

/**
 * Tracker for consuming a single Kinesis stream.
 */
@EqualsAndHashCode
@ToString
public class SingleStreamTracker implements StreamTracker {

    /**
     * By default, single-stream applications should expect the target stream
     * to exist for the duration of the application. Therefore, there is no
     * expectation for the leases to be deleted mid-execution.
     */
    private static final FormerStreamsLeasesDeletionStrategy NO_LEASE_DELETION =
            new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();

    private final StreamIdentifier streamIdentifier;

    private final List<StreamConfig> streamConfigs;

    public SingleStreamTracker(String streamName) {
        this(StreamIdentifier.singleStreamInstance(streamName));
    }

    public SingleStreamTracker(Arn streamArn) {
        this(StreamIdentifier.singleStreamInstance(streamArn));
    }

    public SingleStreamTracker(StreamIdentifier streamIdentifier) {
        this(streamIdentifier, DEFAULT_POSITION_IN_STREAM);
    }

    public SingleStreamTracker(
            StreamIdentifier streamIdentifier, @NonNull InitialPositionInStreamExtended initialPosition) {
        this(streamIdentifier, new StreamConfig(streamIdentifier, initialPosition));
    }

    public SingleStreamTracker(String streamName, @NonNull InitialPositionInStreamExtended initialPosition) {
        this(StreamIdentifier.singleStreamInstance(streamName), initialPosition);
    }

    public SingleStreamTracker(@NonNull StreamIdentifier streamIdentifier, @NonNull StreamConfig streamConfig) {
        this.streamIdentifier = streamIdentifier;
        this.streamConfigs = Collections.singletonList(streamConfig);
    }

    @Override
    public List<StreamConfig> streamConfigList() {
        return streamConfigs;
    }

    @Override
    public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
        return NO_LEASE_DELETION;
    }

    @Override
    public boolean isMultiStream() {
        return false;
    }
}
