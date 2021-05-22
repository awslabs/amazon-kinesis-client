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

package software.amazon.kinesis.leases;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.LeasePendingDeletion;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LeaseCleanupManagerTest {

    private ShardInfo shardInfo;
    private StreamIdentifier streamIdentifier;
    private String concurrencyToken = "1234";

    private String shardId = "shardId";
    private String splitParent = "splitParent";
    private String mergeParent1 = "mergeParent-1";
    private String mergeParent2 = "mergeParent-2";

    private Duration maxFutureWait = Duration.ofSeconds(1);
    private long leaseCleanupIntervalMillis = Duration.ofSeconds(1).toMillis();
    private long completedLeaseCleanupIntervalMillis = Duration.ofSeconds(0).toMillis();
    private long garbageLeaseCleanupIntervalMillis = Duration.ofSeconds(0).toMillis();
    private boolean cleanupLeasesOfCompletedShards = true;
    private LeaseCleanupManager leaseCleanupManager;
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private LeaseCoordinator leaseCoordinator;
    @Mock
    private ShardDetector shardDetector;
    @Mock
    private ScheduledExecutorService deletionThreadPool;

    @Before
    public void setUp() throws Exception {
        shardInfo = new ShardInfo(shardId, concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        streamIdentifier = StreamIdentifier.singleStreamInstance("streamName");
        leaseCleanupManager = new LeaseCleanupManager(leaseCoordinator, NULL_METRICS_FACTORY, deletionThreadPool,
                cleanupLeasesOfCompletedShards, leaseCleanupIntervalMillis, completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis);

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(leaseCoordinator.updateLease(any(Lease.class), any(UUID.class), any(String.class), any(String.class))).thenReturn(true);
    }

    /**
     * Tests subsequent calls to start {@link LeaseCleanupManager}.
     */
    @Test
    public final void testSubsequentStarts() {
        leaseCleanupManager.start();
        Assert.assertTrue(leaseCleanupManager.isRunning());
        leaseCleanupManager.start();
    }

    /**
     * Tests subsequent calls to shutdown {@link LeaseCleanupManager}.
     */
    @Test
    public final void testSubsequentShutdowns() {
        leaseCleanupManager.start();
        Assert.assertTrue(leaseCleanupManager.isRunning());
        leaseCleanupManager.shutdown();
        Assert.assertFalse(leaseCleanupManager.isRunning());
        leaseCleanupManager.shutdown();
    }

    /**
     * Tests that when both child shard leases are present, we are able to delete the parent shard for the completed
     * shard case.
     */
    @Test
    public final void testParentShardLeaseDeletedSplitCase() throws Exception {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForSplit(), ExtendedSequenceNumber.LATEST, 1);
    }

    /**
     * Tests that when both child shard leases are present, we are able to delete the parent shard for the completed
     * shard case.
     */
    @Test
    public final void testParentShardLeaseDeletedMergeCase() throws Exception {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForMerge(), ExtendedSequenceNumber.LATEST, 1);
    }

    /**
     * Tests that if cleanupLeasesOfCompletedShards is not enabled by the customer, then no leases are cleaned up for
     * the completed shard case.
     */
    @Test
    public final void testNoLeasesDeletedWhenNotEnabled() throws Exception {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        cleanupLeasesOfCompletedShards = false;

        leaseCleanupManager = new LeaseCleanupManager(leaseCoordinator, NULL_METRICS_FACTORY, deletionThreadPool,
                cleanupLeasesOfCompletedShards, leaseCleanupIntervalMillis, completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForSplit(), ExtendedSequenceNumber.LATEST, 0);
    }

    /**
     * Tests that if some of the child shard leases are missing, we fail fast and don't delete the parent shard lease
     * for the completed shard case.
     */
    @Test
    public final void testNoCleanupWhenSomeChildShardLeasesAreNotPresent() throws Exception {
        List<ChildShard> childShards = childShardsForSplit();

        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShards, ExtendedSequenceNumber.LATEST, false, 0);
    }

    /**
     * Tests that if some child shard leases haven't begun processing (at least one lease w/ checkpoint TRIM_HORIZON),
     * we don't delete them for the completed shard case.
     */
    @Test
    public final void testParentShardLeaseNotDeletedWhenChildIsAtTrim() throws Exception {
        testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber.TRIM_HORIZON);
    }

    /**
     * Tests that if some child shard leases haven't begun processing (at least one lease w/ checkpoint AT_TIMESTAMP),
     * we don't delete them for the completed shard case.
     */
    @Test
    public final void testParentShardLeaseNotDeletedWhenChildIsAtTimestamp() throws Exception {
        testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber.AT_TIMESTAMP);
    }

    private final void testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber extendedSequenceNumber)
            throws Exception {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForMerge(), extendedSequenceNumber, 0);
    }

    /**
     * Tests that if a lease's parents are still present, we do not delete the lease.
     */
    @Test
    public final void testLeaseNotDeletedWhenParentsStillPresent() throws Exception {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.singleton("parent"),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForMerge(), ExtendedSequenceNumber.LATEST, 0);
    }

    /**
     * Tests ResourceNotFound case for if a shard expires, that we delete the lease when shardExpired is found.
     */
    @Test
    public final void testLeaseDeletedWhenShardDoesNotExist() throws Exception {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        final Lease heldLease = LeaseHelper.createLease(shardInfo.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        testLeaseDeletedWhenShardDoesNotExist(heldLease);
    }

    /**
     * Tests ResourceNotFound case when completed lease cleanup is disabled.
     * @throws Exception
     */
    @Test
    public final void testLeaseDeletedWhenShardDoesNotExistAndCleanupCompletedLeaseDisabled() throws Exception {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        final Lease heldLease = LeaseHelper.createLease(shardInfo.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        cleanupLeasesOfCompletedShards = false;

        leaseCleanupManager = new LeaseCleanupManager(leaseCoordinator, NULL_METRICS_FACTORY, deletionThreadPool,
                cleanupLeasesOfCompletedShards, leaseCleanupIntervalMillis, completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis);

        testLeaseDeletedWhenShardDoesNotExist(heldLease);
    }

    public void testLeaseDeletedWhenShardDoesNotExist(Lease heldLease) throws Exception {
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(leaseCoordinator.getCurrentlyHeldLease(shardInfo.shardId())).thenReturn(heldLease);
        when(shardDetector.getChildShards(any(String.class))).thenThrow(ResourceNotFoundException.class);
        when(leaseRefresher.getLease(heldLease.leaseKey())).thenReturn(heldLease);

        leaseCleanupManager.enqueueForDeletion(new LeasePendingDeletion(streamIdentifier, heldLease, shardInfo, shardDetector));
        leaseCleanupManager.cleanupLeases();

        verify(shardDetector, times(1)).getChildShards(shardInfo.shardId());
        verify(leaseRefresher, times(1)).deleteLease(heldLease);
    }

    private final void verifyExpectedDeletedLeasesCompletedShardCase(ShardInfo shardInfo, List<ChildShard> childShards,
                                                                     ExtendedSequenceNumber extendedSequenceNumber,
                                                                     int expectedDeletedLeases) throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShards, extendedSequenceNumber, true, expectedDeletedLeases);
    }

    private final void verifyExpectedDeletedLeasesCompletedShardCase(ShardInfo shardInfo, List<ChildShard> childShards,
                                                                     ExtendedSequenceNumber extendedSequenceNumber,
                                                                     boolean childShardLeasesPresent,
                                                                     int expectedDeletedLeases) throws Exception {

        final Lease lease = LeaseHelper.createLease(shardInfo.shardId(), "leaseOwner", shardInfo.parentShardIds(),
                childShards.stream().map(c -> c.shardId()).collect(Collectors.toSet()));
        final List<Lease> childShardLeases = childShards.stream().map(c -> LeaseHelper.createLease(
                ShardInfo.getLeaseKey(shardInfo, c.shardId()), "leaseOwner",  Collections.singleton(shardInfo.shardId()),
                Collections.emptyList(), extendedSequenceNumber)).collect(Collectors.toList());

        final List<Lease> parentShardLeases = lease.parentShardIds().stream().map(p ->
                LeaseHelper.createLease(ShardInfo.getLeaseKey(shardInfo, p), "leaseOwner",  Collections.emptyList(),
                        Collections.singleton(shardInfo.shardId()), extendedSequenceNumber)).collect(Collectors.toList());

        when(leaseRefresher.getLease(lease.leaseKey())).thenReturn(lease);
        for (Lease parentShardLease : parentShardLeases) {
            when(leaseRefresher.getLease(parentShardLease.leaseKey())).thenReturn(parentShardLease);
        }
        if (childShardLeasesPresent) {
            for (Lease childShardLease : childShardLeases) {
                when(leaseRefresher.getLease(childShardLease.leaseKey())).thenReturn(childShardLease);
            }
        }

        leaseCleanupManager.enqueueForDeletion(new LeasePendingDeletion(streamIdentifier, lease, shardInfo, shardDetector));
        leaseCleanupManager.cleanupLeases();

        verify(shardDetector, times(1)).getChildShards(shardInfo.shardId());
        verify(leaseRefresher, times(expectedDeletedLeases)).deleteLease(any(Lease.class));
    }

    private List<ChildShard> childShardsForSplit() {
        List<String> parentShards = Arrays.asList(splitParent);

        ChildShard leftChild = ChildShard.builder()
                .shardId("leftChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
        ChildShard rightChild = ChildShard.builder()
                .shardId("rightChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"))
                .build();

        return  Arrays.asList(leftChild, rightChild);
    }

    private List<ChildShard> childShardsForMerge() {
        List<String> parentShards = Arrays.asList(mergeParent1, mergeParent2);

        ChildShard child = ChildShard.builder()
                .shardId("onlyChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "99"))
                .build();

        return Collections.singletonList(child);
    }
}
