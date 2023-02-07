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

package software.amazon.kinesis.common;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import software.amazon.awssdk.utils.Either;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;

public class DeprecationUtilsTest {

    @Test
    public void testTrackerConversion() {
        final StreamTracker mockMultiTracker = mock(MultiStreamTracker.class);
        assertEquals(Either.left(mockMultiTracker), DeprecationUtils.convert(mockMultiTracker, Function.identity()));

        final StreamTracker mockSingleTracker = mock(SingleStreamTracker.class);
        assertEquals(Either.right(mockSingleTracker), DeprecationUtils.convert(mockSingleTracker, Function.identity()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedStreamTrackerConversion() {
        DeprecationUtils.convert(mock(StreamTracker.class), Function.identity());
    }

}