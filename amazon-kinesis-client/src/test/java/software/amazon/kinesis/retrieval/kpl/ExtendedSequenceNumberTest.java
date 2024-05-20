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
package software.amazon.kinesis.retrieval.kpl;

import org.junit.Test;
import software.amazon.kinesis.checkpoint.SentinelCheckpoint;

import static org.junit.Assert.assertTrue;

public class ExtendedSequenceNumberTest {

    @Test
    public void testSentinelCheckpoints() {
        for (final SentinelCheckpoint sentinel : SentinelCheckpoint.values()) {
            final ExtendedSequenceNumber esn = new ExtendedSequenceNumber(sentinel.name());
            assertTrue(sentinel.name(), esn.isSentinelCheckpoint());

            // For backwards-compatibility, sentinels should ignore subsequences
            final ExtendedSequenceNumber esnWithSubsequence = new ExtendedSequenceNumber(sentinel.name(), 42L);
            assertTrue(sentinel.name(), esnWithSubsequence.isSentinelCheckpoint());
        }
    }
}
