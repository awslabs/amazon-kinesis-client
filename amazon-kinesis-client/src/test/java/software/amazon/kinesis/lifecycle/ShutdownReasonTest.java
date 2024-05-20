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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests of ShutdownReason enum class.
 */
public class ShutdownReasonTest {

    @Test
    public void testTransitionZombie() {
        Assert.assertFalse(ShutdownReason.LEASE_LOST.canTransitionTo(ShutdownReason.SHARD_END));
        assertFalse(ShutdownReason.LEASE_LOST.canTransitionTo(ShutdownReason.REQUESTED));
    }

    @Test
    public void testTransitionTerminate() {
        assertTrue(ShutdownReason.SHARD_END.canTransitionTo(ShutdownReason.LEASE_LOST));
        assertFalse(ShutdownReason.SHARD_END.canTransitionTo(ShutdownReason.REQUESTED));
    }

    @Test
    public void testTransitionRequested() {
        assertTrue(ShutdownReason.REQUESTED.canTransitionTo(ShutdownReason.LEASE_LOST));
        assertTrue(ShutdownReason.REQUESTED.canTransitionTo(ShutdownReason.SHARD_END));
    }
}
