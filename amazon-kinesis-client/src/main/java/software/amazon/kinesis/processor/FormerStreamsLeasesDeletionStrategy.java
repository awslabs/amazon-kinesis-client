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

import java.time.Duration;
import java.util.List;

import software.amazon.kinesis.common.StreamIdentifier;

/**
 * Strategy for cleaning up the leases for former streams.
 */
public interface FormerStreamsLeasesDeletionStrategy {

    /**
     * StreamIdentifiers for which leases needs to be cleaned up in the lease table.
     * @return
     */
    List<StreamIdentifier> streamIdentifiersForLeaseCleanup();

    /**
     * Duration to wait before deleting the leases for this stream.
     * @return
     */
    Duration waitPeriodToDeleteFormerStreams();

    /**
     * Strategy type for deleting the leases of former active streams.
     * @return
     */
    StreamsLeasesDeletionType leaseDeletionType();

    /**
     * StreamsLeasesDeletionType identifying the different lease cleanup strategies.
     */
    enum StreamsLeasesDeletionType {
        NO_STREAMS_LEASES_DELETION,
        FORMER_STREAMS_AUTO_DETECTION_DEFERRED_DELETION,
        PROVIDED_STREAMS_DEFERRED_DELETION
    }

    /**
     * Strategy for not cleaning up leases for former streams.
     */
    final class NoLeaseDeletionStrategy implements FormerStreamsLeasesDeletionStrategy {

        @Override
        public final List<StreamIdentifier> streamIdentifiersForLeaseCleanup() {
            throw new UnsupportedOperationException("StreamIdentifiers not required");
        }

        @Override
        public final Duration waitPeriodToDeleteFormerStreams() {
            return Duration.ZERO;
        }

        @Override
        public final StreamsLeasesDeletionType leaseDeletionType() {
            return StreamsLeasesDeletionType.NO_STREAMS_LEASES_DELETION;
        }
    }

    /**
     * Strategy for auto detection the old of former streams based on the {@link MultiStreamTracker#streamConfigList()}
     * and do deferred deletion based on {@link #waitPeriodToDeleteFormerStreams()}
     */
    abstract class AutoDetectionAndDeferredDeletionStrategy implements FormerStreamsLeasesDeletionStrategy {

        @Override
        public final List<StreamIdentifier> streamIdentifiersForLeaseCleanup() {
            throw new UnsupportedOperationException("StreamIdentifiers not required");
        }

        @Override
        public final StreamsLeasesDeletionType leaseDeletionType() {
            return StreamsLeasesDeletionType.FORMER_STREAMS_AUTO_DETECTION_DEFERRED_DELETION;
        }
    }

    /**
     * Strategy to detect the streams for deletion through {@link #streamIdentifiersForLeaseCleanup()} provided by customer at runtime
     * and do deferred deletion based on {@link #waitPeriodToDeleteFormerStreams()}
     */
    abstract class ProvidedStreamsDeferredDeletionStrategy implements FormerStreamsLeasesDeletionStrategy {

        @Override
        public final StreamsLeasesDeletionType leaseDeletionType() {
            return StreamsLeasesDeletionType.PROVIDED_STREAMS_DEFERRED_DELETION;
        }
    }
}
