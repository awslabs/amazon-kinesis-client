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

package com.amazonaws.services.kinesis.leases.impl;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardObjectHelper;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.LeasePendingDeletion;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.ChildShard;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LeaseCleanupManagerTest {

    private ShardInfo shardInfo;
    private String concurrencyToken = "1234";
    private int maxRecords = 1;

    private String getShardId = "getShardId";
    private String splitParent = "splitParent";
    private String mergeParent1 = "mergeParent-1";
    private String mergeParent2 = "mergeParent-2";

    private long leaseCleanupIntervalMillis = Duration.ofSeconds(1).toMillis();
    private long completedLeaseCleanupIntervalMillis = Duration.ofSeconds(0).toMillis();
    private long garbageLeaseCleanupIntervalMillis = Duration.ofSeconds(0).toMillis();
    private boolean cleanupLeasesOfCompletedShards = true;
    private LeaseCleanupManager leaseCleanupManager;
    private static final IMetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    @Mock
    private LeaseManager leaseManager;
    @Mock
    private LeaseCoordinator leaseCoordinator;
    @Mock
    private IKinesisProxy kinesis;
    @Mock
    private ScheduledExecutorService deletionThreadPool;

    @Before
    public void setUp() {
        shardInfo = new ShardInfo(getShardId, concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        leaseCleanupManager = new LeaseCleanupManager(kinesis, leaseManager, deletionThreadPool, NULL_METRICS_FACTORY,
                cleanupLeasesOfCompletedShards, leaseCleanupIntervalMillis, completedLeaseCleanupIntervalMillis, garbageLeaseCleanupIntervalMillis, maxRecords);
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
     * Tests that when both child shard leases are present, we are able to delete the parent shard for the completed
     * shard case.
     */
    @Test
    public final void testParentShardLeaseDeletedSplitCase() throws Exception {
        shardInfo = new ShardInfo("ShardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForSplit(), ExtendedSequenceNumber.LATEST, 1);
    }

    /**
     * Tests that when both child shard leases are present, we are able to delete the parent shard for the completed
     * shard case.
     */
    @Test
    public final void testParentShardLeaseDeletedMergeCase() throws Exception {
        shardInfo = new ShardInfo("ShardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForMerge(), ExtendedSequenceNumber.LATEST, 1);
    }

    /**
     * Tests that if cleanupLeasesOfCompletedShards is not enabled by the customer, then no leases are cleaned up for
     * the completed shard case.
     */
    @Test
    public final void testNoLeasesDeletedWhenNotEnabled() throws Exception {
        shardInfo = new ShardInfo("ShardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        cleanupLeasesOfCompletedShards = false;

        leaseCleanupManager = new LeaseCleanupManager(kinesis, leaseManager, deletionThreadPool, NULL_METRICS_FACTORY,
                cleanupLeasesOfCompletedShards, leaseCleanupIntervalMillis, completedLeaseCleanupIntervalMillis, garbageLeaseCleanupIntervalMillis, maxRecords);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForSplit(), ExtendedSequenceNumber.LATEST, 0);
    }

    /**
     * Tests that if some of the child shard leases are missing, we fail fast and don't delete the parent shard lease
     * for the completed shard case.
     */
    @Test
    public final void testNoCleanupWhenSomeChildShardLeasesAreNotPresent() throws Exception {
        List<ChildShard> childShards = childShardsForSplit();

        shardInfo = new ShardInfo("ShardId-0", concurrencyToken, Collections.emptySet(),
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

    private  void testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber extendedSequenceNumber)
            throws Exception {
        shardInfo = new ShardInfo("ShardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForMerge(), extendedSequenceNumber, 0);
    }

    /**
     * Tests that if a lease's parents are still present, we do not delete the lease.
     */
    @Test
    public final void testLeaseNotDeletedWhenParentsStillPresent() throws Exception {
        shardInfo = new ShardInfo("ShardId-0", concurrencyToken, Collections.singleton("parent"),
                ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShardsForMerge(), ExtendedSequenceNumber.LATEST, 0);
    }

    /**
     * Tests ResourceNotFound case for if a shard expires, that we delete the lease when shardExpired is found.
     */
    @Test
    public final void testLeaseDeletedWhenShardDoesNotExist() throws Exception {
        shardInfo = new ShardInfo("ShardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        final KinesisClientLease heldLease = LeaseHelper.createLease(shardInfo.getShardId(), "leaseOwner", Collections.singleton("parentShardId"));

        testLeaseDeletedWhenShardDoesNotExist(heldLease);
    }

    /**
     * Tests ResourceNotFound case when completed lease cleanup is disabled.
     * @throws Exception
     */
    @Test
    public final void testLeaseDeletedWhenShardDoesNotExistAndCleanupCompletedLeaseDisabled() throws Exception {
        shardInfo = new ShardInfo("ShardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        final KinesisClientLease heldLease = LeaseHelper.createLease(shardInfo.getShardId(), "leaseOwner", Collections.singleton("parentShardId"));

        cleanupLeasesOfCompletedShards = false;

        leaseCleanupManager = new LeaseCleanupManager(kinesis, leaseManager, deletionThreadPool, NULL_METRICS_FACTORY,
                cleanupLeasesOfCompletedShards, leaseCleanupIntervalMillis, completedLeaseCleanupIntervalMillis, garbageLeaseCleanupIntervalMillis, maxRecords);

        testLeaseDeletedWhenShardDoesNotExist(heldLease);
    }

    public void testLeaseDeletedWhenShardDoesNotExist(KinesisClientLease heldLease) throws Exception {
        when(leaseCoordinator.getCurrentlyHeldLease(shardInfo.getShardId())).thenReturn(heldLease);
        when(kinesis.get(anyString(), anyInt())).thenThrow(ResourceNotFoundException.class);
        when(kinesis.getIterator(anyString(), anyString())).thenThrow(ResourceNotFoundException.class);
        when(leaseManager.getLease(heldLease.getLeaseKey())).thenReturn(heldLease);

        leaseCleanupManager.enqueueForDeletion(new LeasePendingDeletion(heldLease, shardInfo));
        leaseCleanupManager.cleanupLeases();

        verify(leaseManager, times(1)).deleteLease(heldLease);
    }

    private void verifyExpectedDeletedLeasesCompletedShardCase(ShardInfo shardInfo, List<ChildShard> childShards,
                                                                     ExtendedSequenceNumber extendedSequenceNumber,
                                                                     int expectedDeletedLeases) throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(shardInfo, childShards, extendedSequenceNumber, true, expectedDeletedLeases);
    }

    private void verifyExpectedDeletedLeasesCompletedShardCase(ShardInfo shardInfo, List<ChildShard> childShards,
                                                                     ExtendedSequenceNumber extendedSequenceNumber,
                                                                     boolean childShardLeasesPresent,
                                                                     int expectedDeletedLeases) throws Exception {

        final KinesisClientLease lease = LeaseHelper.createLease(shardInfo.getShardId(), "leaseOwner", shardInfo.getParentShardIds(),
                childShards.stream().map(c -> c.getShardId()).collect(Collectors.toSet()));
        final List<KinesisClientLease> childShardLeases = childShards.stream().map(c -> LeaseHelper.createLease(
                c.getShardId(), "leaseOwner",  Collections.singleton(shardInfo.getShardId()),
                Collections.emptyList(), extendedSequenceNumber)).collect(Collectors.toList());

        final List<KinesisClientLease> parentShardLeases = lease.getParentShardIds().stream().map(p ->
                LeaseHelper.createLease(p, "leaseOwner",  Collections.emptyList(),
                        Collections.singleton(shardInfo.getShardId()), extendedSequenceNumber)).collect(Collectors.toList());

        when(leaseManager.getLease(lease.getLeaseKey())).thenReturn(lease);
        for (Lease parentShardLease : parentShardLeases) {
            when(leaseManager.getLease(parentShardLease.getLeaseKey())).thenReturn(parentShardLease);
        }
        if (childShardLeasesPresent) {
            for (Lease childShardLease : childShardLeases) {
                when(leaseManager.getLease(childShardLease.getLeaseKey())).thenReturn(childShardLease);
            }
        }

        when(kinesis.getIterator(any(String.class), any(String.class))).thenReturn("123");

        final GetRecordsResult getRecordsResult = new GetRecordsResult();
                getRecordsResult.setRecords(Collections.emptyList());
                getRecordsResult.setChildShards(childShards);

        when(kinesis.get(any(String.class), any(Integer.class))).thenReturn(getRecordsResult);

        leaseCleanupManager.enqueueForDeletion(new LeasePendingDeletion(lease, shardInfo));
        leaseCleanupManager.cleanupLeases();

        verify(leaseManager, times(expectedDeletedLeases)).deleteLease(any(Lease.class));
    }

    private List<ChildShard> childShardsForSplit() {
        final List<String> parentShards = Arrays.asList(splitParent);

        final ChildShard leftChild = new ChildShard();
                leftChild.setShardId("leftChild");
                leftChild.setParentShards(parentShards);
                leftChild.setHashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"));

        final ChildShard rightChild = new ChildShard();
                rightChild.setShardId("rightChild");
                rightChild.setParentShards(parentShards);
                rightChild.setHashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"));

        return  Arrays.asList(leftChild, rightChild);
    }

    private List<ChildShard> childShardsForMerge() {
        final List<String> parentShards = Arrays.asList(mergeParent1, mergeParent2);

        final ChildShard child = new ChildShard();
                child.setShardId("onlyChild");
                child.setParentShards(parentShards);
                child.setHashKeyRange(ShardObjectHelper.newHashKeyRange("0", "99"));

        return Collections.singletonList(child);
    }
}
