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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 *
 */
public class BlockOnParentShardTaskTest {
    private final long backoffTimeInMillis = 50L;
    private final String shardId = "shardId-97";
    private final String streamId = "123:stream:146";
    private final String concurrencyToken = "testToken";
    private final List<String> emptyParentShardIds = new ArrayList<>();
    private ShardInfo shardInfo;

    @Before
    public void setup() {
        shardInfo = new ShardInfo(shardId, concurrencyToken, emptyParentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
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
        LeaseRefresher leaseRefresher = mock(LeaseRefresher.class);
        when(leaseRefresher.getLease(shardId)).thenReturn(null);

        BlockOnParentShardTask task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        TaskResult result = task.call();
        assertNull(result.getException());
    }

    /**
     * Test call() when there are 1-2 parent shards that have been fully processed.
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    @Test
    public final void testCallShouldNotThrowBlockedOnParentWhenParentsHaveFinished()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ShardInfo shardInfo = null;
        BlockOnParentShardTask task = null;
        String parent1ShardId = "shardId-1";
        String parent2ShardId = "shardId-2";
        List<String> parentShardIds = new ArrayList<>();
        TaskResult result = null;

        Lease parent1Lease = new Lease();
        parent1Lease.checkpoint(ExtendedSequenceNumber.SHARD_END);
        Lease parent2Lease = new Lease();
        parent2Lease.checkpoint(ExtendedSequenceNumber.SHARD_END);

        LeaseRefresher leaseRefresher = mock(LeaseRefresher.class);
        when(leaseRefresher.getLease(parent1ShardId)).thenReturn(parent1Lease);
        when(leaseRefresher.getLease(parent2ShardId)).thenReturn(parent2Lease);

        // test single parent
        parentShardIds.add(parent1ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNull(result.getException());

        // test two parents
        parentShardIds.add(parent2ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNull(result.getException());
    }

    /**
     * Test call() when there are 1-2 parent shards that have been fully processed.
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    @Test
    public final void testCallShouldNotThrowBlockedOnParentWhenParentsHaveFinishedMultiStream()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ShardInfo shardInfo = null;
        BlockOnParentShardTask task = null;
        String parent1LeaseKey = streamId + ":" + "shardId-1";
        String parent2LeaseKey = streamId + ":" + "shardId-2";
        String parent1ShardId = "shardId-1";
        String parent2ShardId = "shardId-2";
        List<String> parentShardIds = new ArrayList<>();
        TaskResult result = null;

        Lease parent1Lease = new Lease();
        parent1Lease.checkpoint(ExtendedSequenceNumber.SHARD_END);
        Lease parent2Lease = new Lease();
        parent2Lease.checkpoint(ExtendedSequenceNumber.SHARD_END);

        LeaseRefresher leaseRefresher = mock(LeaseRefresher.class);
        when(leaseRefresher.getLease(parent1LeaseKey)).thenReturn(parent1Lease);
        when(leaseRefresher.getLease(parent2LeaseKey)).thenReturn(parent2Lease);

        // test single parent
        parentShardIds.add(parent1ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON,
                streamId);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNull(result.getException());

        // test two parents
        parentShardIds.add(parent2ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON, streamId);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNull(result.getException());
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

        Lease parent1Lease = new Lease();
        parent1Lease.checkpoint(ExtendedSequenceNumber.LATEST);
        Lease parent2Lease = new Lease();
        // mock a sequence number checkpoint
        parent2Lease.checkpoint(new ExtendedSequenceNumber("98182584034"));

        LeaseRefresher leaseRefresher = mock(LeaseRefresher.class);
        when(leaseRefresher.getLease(parent1ShardId)).thenReturn(parent1Lease);
        when(leaseRefresher.getLease(parent2ShardId)).thenReturn(parent2Lease);

        // test single parent
        parentShardIds.add(parent1ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNotNull(result.getException());

        // test two parents
        parentShardIds.add(parent2ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNotNull(result.getException());
    }

    /**
     * Test call() when there are 1-2 parent shards that have NOT been fully processed.
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    @Test
    public final void testCallWhenParentsHaveNotFinishedMultiStream()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {

        ShardInfo shardInfo = null;
        BlockOnParentShardTask task = null;
        String parent1LeaseKey = streamId + ":" + "shardId-1";
        String parent2LeaseKey = streamId + ":" + "shardId-2";
        String parent1ShardId = "shardId-1";
        String parent2ShardId = "shardId-2";
        List<String> parentShardIds = new ArrayList<>();
        TaskResult result = null;

        Lease parent1Lease = new Lease();
        parent1Lease.checkpoint(ExtendedSequenceNumber.LATEST);
        Lease parent2Lease = new Lease();
        // mock a sequence number checkpoint
        parent2Lease.checkpoint(new ExtendedSequenceNumber("98182584034"));

        LeaseRefresher leaseRefresher = mock(LeaseRefresher.class);
        when(leaseRefresher.getLease(parent1LeaseKey)).thenReturn(parent1Lease);
        when(leaseRefresher.getLease(parent2LeaseKey)).thenReturn(parent2Lease);

        // test single parent
        parentShardIds.add(parent1ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON, streamId);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNotNull(result.getException());

        // test two parents
        parentShardIds.add(parent2ShardId);
        shardInfo = new ShardInfo(shardId, concurrencyToken, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON, streamId);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNotNull(result.getException());
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
        Lease parentLease = new Lease();
        LeaseRefresher leaseRefresher = mock(LeaseRefresher.class);
        when(leaseRefresher.getLease(parentShardId)).thenReturn(parentLease);

        // test when parent shard has not yet been fully processed
        parentLease.checkpoint(new ExtendedSequenceNumber("98182584034"));
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNotNull(result.getException());

        // test when parent has been fully processed
        parentLease.checkpoint(ExtendedSequenceNumber.SHARD_END);
        task = new BlockOnParentShardTask(shardInfo, leaseRefresher, backoffTimeInMillis);
        result = task.call();
        assertNull(result.getException());
    }

    /**
     * Test to verify we return the right task type.
     */
    @Test
    public final void testGetTaskType() {
        BlockOnParentShardTask task = new BlockOnParentShardTask(shardInfo, null, backoffTimeInMillis);
        assertEquals(TaskType.BLOCK_ON_PARENT_SHARDS, task.taskType());
    }

}
