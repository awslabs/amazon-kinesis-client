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

import org.hamcrest.Matchers;
import org.junit.Test;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class SingleStreamTrackerTest {

    private static final String STREAM_NAME = SingleStreamTrackerTest.class.getSimpleName();

    @Test
    public void testDefaults() {
        validate(new SingleStreamTracker(STREAM_NAME));
        validate(new SingleStreamTracker(StreamIdentifier.singleStreamInstance(STREAM_NAME)));
    }

    @Test
    public void testInitialPositionConstructor() {
        final InitialPositionInStreamExtended expectedPosition =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
        assertNotEquals(expectedPosition, StreamTracker.DEFAULT_POSITION_IN_STREAM);

        final StreamTracker tracker =
                new SingleStreamTracker(StreamIdentifier.singleStreamInstance(STREAM_NAME), expectedPosition);
        validate(tracker, expectedPosition);
    }

    private static void validate(StreamTracker tracker) {
        validate(tracker, StreamTracker.DEFAULT_POSITION_IN_STREAM);
    }

    private static void validate(StreamTracker tracker, InitialPositionInStreamExtended expectedPosition) {
        assertEquals(1, tracker.streamConfigList().size());
        assertFalse(tracker.isMultiStream());
        assertThat(
                tracker.formerStreamsLeasesDeletionStrategy(),
                Matchers.instanceOf(FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy.class));

        final StreamConfig config = tracker.streamConfigList().get(0);
        assertEquals(STREAM_NAME, config.streamIdentifier().streamName());
        assertEquals(expectedPosition, config.initialPositionInStreamExtended());
    }
}
