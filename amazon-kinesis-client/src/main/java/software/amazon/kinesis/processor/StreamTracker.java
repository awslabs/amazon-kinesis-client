/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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

import java.util.List;

import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;

/**
 * Interface for stream trackers.
 * KCL will periodically probe this interface to learn about the new and old streams.
 */
public interface StreamTracker {

    /**
     * Default position to begin consuming records from a Kinesis stream.
     *
     * @see #orphanedStreamInitialPositionInStream()
     */
    InitialPositionInStreamExtended DEFAULT_POSITION_IN_STREAM =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

    /**
     * Returns the list of stream config, to be processed by the current application.
     * <b>Note that the streams list CAN be changed during the application runtime.</b>
     * This method will be called periodically by the KCL to learn about the change in streams to process.
     *
     * @return List of StreamConfig
     */
    List<StreamConfig> streamConfigList();

    /**
     * Strategy to delete leases of old streams in the lease table.
     * <b>Note that the strategy CANNOT be changed during the application runtime.</b>
     *
     * @return StreamsLeasesDeletionStrategy
     */
    FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy();

    /**
     * The position for getting records from an "orphaned" stream that is in the lease table but not tracked
     * Default assumes that the stream no longer need to be tracked, so use LATEST for faster shard end.
     *
     * <p>Default value: {@link InitialPositionInStream#LATEST}</p>
     */
    default InitialPositionInStreamExtended orphanedStreamInitialPositionInStream() {
        return DEFAULT_POSITION_IN_STREAM;
    }

    /**
     * Returns a new {@link StreamConfig} for the provided stream identifier.
     *
     * @param streamIdentifier stream for which to create a new config
     */
    default StreamConfig createStreamConfig(StreamIdentifier streamIdentifier) {
        return new StreamConfig(streamIdentifier, orphanedStreamInitialPositionInStream());
    }

    /**
     * Returns true if this application should accommodate the consumption of
     * more than one Kinesis stream.
     * <p>
     * <b>This method must be consistent.</b> Varying the returned value will
     * have indeterminate, and likely problematic, effects on stream processing.
     * </p>
     */
    boolean isMultiStream();
}
