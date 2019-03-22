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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

/**
 *
 */
public class BlockOnParentShardTaskTest {

    private static final Log LOG = LogFactory.getLog(BlockOnParentShardTaskTest.class);
    private final long backoffTimeInMillis = 50L;
    private final String shardId = "shardId-97";
    private final String concurrencyToken = "testToken";
    private final List<String> emptyParentShardIds = new ArrayList<String>();
    ShardInfo defaultShardInfo = new ShardInfo(shardId, concurrencyToken, emptyParentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test call() when there are no parent shards.
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    @Test
    public final void testCallNoParents()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        when(leaseManager.getLease(shardId)).thenReturn(null);

        BlockOnParentShardTask task = new BlockOnParentShardTask(defaultShardInfo, leaseManager, backoffTimeInMillis);
        TaskResult result = task.call();
        Assert.assertNull(result.getException());
    }

    /**
     * Test call() when there are 1-2 parent shards that have been fully processed.
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    @Test
    public final void testCallWhenParentsHaveFinished()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {

        ShardInfo shardInfo = null;
        BlockOnParentShardTask task = null;
        String parent1ShardId = "shardId-1";
        String parent2ShardId = "shardId-2";
        List<String> parentShardIds = new ArrayList<>();
        TaskResult result = null;

        KinesisClientLease parent1Lease = new KinesisClientLease();
        parent1Lease.setCheckpoint(ExtendedSequenceNumber.SHARD_END);
        KinesisClientLease parent2Lease = new KinesisClientLease();
        parent2Lease.setCheckpoint(ExtendedSequenceNumber.SHARD_END);

        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        when(leaseManager.getLease(parent1ShardId)).thenReturn(parent1Lease);
        when(leaseManager.getLease(parent2ShardId)).thenReturn(parent2Lease);

        // test single parent
        parentShardIds.add(parent1ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        task = new BlockOnParentShardTask(shardInfo, leaseManager, backoffTimeInMillis);
        result = task.call();
        Assert.assertNull(result.getException());

        // test two parents
        parentShardIds.add(parent2ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        task = new BlockOnParentShardTask(shardInfo, leaseManager, backoffTimeInMillis);
        result = task.call();
        Assert.assertNull(result.getException());
    }

    /**
     * Test call() when there are 1-2 parent shards that have NOT been fully processed.
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    @Test
    public final void testCallWhenParentsHaveNotFinished()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {

        ShardInfo shardInfo = null;
        BlockOnParentShardTask task = null;
        String parent1ShardId = "shardId-1";
        String parent2ShardId = "shardId-2";
        List<String> parentShardIds = new ArrayList<>();
        TaskResult result = null;

        KinesisClientLease parent1Lease = new KinesisClientLease();
        parent1Lease.setCheckpoint(ExtendedSequenceNumber.LATEST);
        KinesisClientLease parent2Lease = new KinesisClientLease();
        // mock a sequence number checkpoint
        parent2Lease.setCheckpoint(new ExtendedSequenceNumber("98182584034"));

        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        when(leaseManager.getLease(parent1ShardId)).thenReturn(parent1Lease);
        when(leaseManager.getLease(parent2ShardId)).thenReturn(parent2Lease);

        // test single parent
        parentShardIds.add(parent1ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        task = new BlockOnParentShardTask(shardInfo, leaseManager, backoffTimeInMillis);
        result = task.call();
        Assert.assertNotNull(result.getException());

        // test two parents
        parentShardIds.add(parent2ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        task = new BlockOnParentShardTask(shardInfo, leaseManager, backoffTimeInMillis);
        result = task.call();
        Assert.assertNotNull(result.getException());
    }

    /**
     * Test call() with 1 parent shard before and after it is completely processed.
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    @Test
    public final void testCallBeforeAndAfterAParentFinishes()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {

        BlockOnParentShardTask task = null;
        String parentShardId = "shardId-1";
        List<String> parentShardIds = new ArrayList<>();
        parentShardIds.add(parentShardId);
        ShardInfo shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        TaskResult result = null;
        KinesisClientLease parentLease = new KinesisClientLease();
        ILeaseManager<KinesisClientLease> leaseManager = mock(ILeaseManager.class);
        when(leaseManager.getLease(parentShardId)).thenReturn(parentLease);

        // test when parent shard has not yet been fully processed
        parentLease.setCheckpoint(new ExtendedSequenceNumber("98182584034"));
        task = new BlockOnParentShardTask(shardInfo, leaseManager, backoffTimeInMillis);
        result = task.call();
        Assert.assertNotNull(result.getException());

        // test when parent has been fully processed
        parentLease.setCheckpoint(ExtendedSequenceNumber.SHARD_END);
        task = new BlockOnParentShardTask(shardInfo, leaseManager, backoffTimeInMillis);
        result = task.call();
        Assert.assertNull(result.getException());
    }

    /**
     * Test to verify we return the right task type.
     */
    @Test
    public final void testGetTaskType() {
        BlockOnParentShardTask task = new BlockOnParentShardTask(defaultShardInfo, null, backoffTimeInMillis);
        Assert.assertEquals(TaskType.BLOCK_ON_PARENT_SHARDS, task.getTaskType());
    }

}
