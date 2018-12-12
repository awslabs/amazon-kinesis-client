/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import junit.framework.Assert;

import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;

public class CheckpointValueComparatorTest {
    @Test
    public final void testCheckpointValueComparator() {
        CheckpointValueComparator comparator = new CheckpointValueComparator();
        final String trimHorizon = SentinelCheckpoint.TRIM_HORIZON.toString();
        final String latest = SentinelCheckpoint.LATEST.toString();
        final String shardEnd = SentinelCheckpoint.SHARD_END.toString();
        final String lesser = "17";
        final String greater = "123";
        final String notASentinelCheckpointValue = "just-some-string";

        String[][] equalValues =
        { { trimHorizon, trimHorizon }, { latest, latest }, { greater, greater }, { shardEnd, shardEnd } };

        // Check equal values
        for (String[] pair : equalValues) {
            Assert.assertTrue("Expected: " + pair[0] + " and " + pair[1] + " to be equal",
                    comparator.compare(pair[0], pair[1]) == 0 && comparator.compare(pair[1], pair[0]) == 0);

        }

        // Check non-equal values
        String[][] lessThanValues =
        { { latest, lesser }, { trimHorizon, greater }, { lesser, greater },
                { trimHorizon, shardEnd }, { latest, shardEnd }, { lesser, shardEnd }, { trimHorizon, latest } };
        for (String[] pair : lessThanValues) {
            Assert.assertTrue("Expected: " + pair[0] + " < " + pair[1],
                    comparator.compare(pair[0], pair[1]) < 0);
            Assert.assertTrue("Expected: " + pair[1] + " > " + pair[0],
                    comparator.compare(pair[1], pair[0]) > 0);
        }

        // Check bad values
        String[][] badValues =
        { { null, null }, { latest, null }, { null, trimHorizon }, { null, shardEnd }, { null, lesser },
                { null, notASentinelCheckpointValue }, { latest, notASentinelCheckpointValue },
                { notASentinelCheckpointValue, trimHorizon }, { shardEnd, notASentinelCheckpointValue },
                { notASentinelCheckpointValue, lesser } };
        for (String[] pair : badValues) {
            try {
                comparator.compare(pair[0], pair[1]);
                Assert.fail("Compare should have thrown an exception when one of its parameters is not a sequence "
                        + "number and not a sentinel checkpoint value but didn't when comparing " + pair[0] + " and "
                        + pair[1]);
            } catch (Exception e1) {
                try {
                    comparator.compare(pair[1], pair[0]);
                    Assert.fail("Compare should have thrown an exception when one of its parameters is not a sequence "
                            + "number and not a sentinel checkpoint value but didn't when comparing " + pair[1]
                            + " and " + pair[0]);
                } catch (Exception e2) {
                    continue;
                }
            }
        }
    }
}
