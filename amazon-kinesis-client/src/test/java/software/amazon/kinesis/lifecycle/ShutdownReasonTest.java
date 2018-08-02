/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

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
