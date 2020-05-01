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

package software.amazon.kinesis.coordinator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.common.HashKeyRangeForLease;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static software.amazon.kinesis.common.HashKeyRangeForLease.deserialize;

@RunWith(MockitoJUnitRunner.class)

public class PeriodicShardSyncManagerTest {

    @Before
    public void setup() {

    }

    @Test
    public void testIfHashRangesAreNotMergedWhenNoOverlappingIntervalsGiven() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRanges);
        Assert.assertEquals(hashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testIfHashRangesAreSortedWhenNoOverlappingIntervalsGiven() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("2", "3"));
            add(deserialize("0", "1"));
            add(deserialize("24", "30"));
            add(deserialize("4", "23"));
        }};
        List<HashKeyRangeForLease> hashRangesCopy = new ArrayList<>();
        hashRangesCopy.addAll(hashRanges);
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRangesCopy);
        Assert.assertEquals(hashRangesCopy, sortAndMergedHashRanges);
        Assert.assertNotEquals(hashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testIfHashRangesAreMergedWhenOverlappingIntervalsGivenCase1() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> expectedHashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRanges);
        Assert.assertEquals(expectedHashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testIfHashRangesAreMergedWhenOverlappingIntervalsGivenCase2() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "5"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> expectedHashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRanges);
        Assert.assertEquals(expectedHashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testIfHashRangesAreMergedWhenOverlappingIntervalsGivenCase3() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("4", "5"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> expectedHashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRanges);
        Assert.assertEquals(expectedHashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testForFailureWhenHashRangesAreIncomplete() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("25", "30")); // Missing interval here
        }};
        Assert.assertTrue(PeriodicShardSyncManager
                .checkForHoleInHashKeyRanges(hashRanges, BigInteger.ZERO, BigInteger.valueOf(30)).isPresent());
    }

    @Test
    public void testForSuccessWhenHashRangesAreComplete() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", "30"));
        }};
        Assert.assertFalse(PeriodicShardSyncManager
                .checkForHoleInHashKeyRanges(hashRanges, BigInteger.ZERO, BigInteger.valueOf(30)).isPresent());
    }

}
