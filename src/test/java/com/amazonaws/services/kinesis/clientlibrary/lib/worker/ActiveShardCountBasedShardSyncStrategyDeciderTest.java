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

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.ActiveShardCountBasedShardSyncStrategyDecider.MIN_ACTIVE_SHARDS_FOR_PERIODIC_SYNC;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

@RunWith(MockitoJUnitRunner.class)
public class ActiveShardCountBasedShardSyncStrategyDeciderTest extends PeriodicShardSyncTestBase {
    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;

    @Test
    public void testShardEndStrategyResultReturnedWhenActiveLeasesLessThanRequired()
            throws Exception {
        List<KinesisClientLease> leases = getLeases(MIN_ACTIVE_SHARDS_FOR_PERIODIC_SYNC - 1,
                false /* duplicateLeaseOwner */, true /* activeLeases */);
        when(leaseManager.listLeases()).thenReturn(leases);
        ShardSyncStrategyDecider shardSyncStrategyDecider = new ActiveShardCountBasedShardSyncStrategyDecider(leaseManager);
        ShardSyncStrategy actualShardSyncStrategy = shardSyncStrategyDecider.getShardSyncStrategy();
        assertEquals(String.format("%s Shard sync strategy expected", ShardSyncStrategy.SHARD_END.toString()), ShardSyncStrategy.SHARD_END,
                actualShardSyncStrategy);
    }

    @Test
    public void testPeriodicStrategyResultReturnedWhenActiveLeasesGreaterThanRequired() throws Exception {
        List<KinesisClientLease> leases = getLeases(MIN_ACTIVE_SHARDS_FOR_PERIODIC_SYNC + 1,
                false /* duplicateLeaseOwner */, true /* activeLeases */);
        when(leaseManager.listLeases()).thenReturn(leases);
        ShardSyncStrategyDecider shardSyncStrategyDecider = new ActiveShardCountBasedShardSyncStrategyDecider(
                leaseManager);
        ShardSyncStrategy actualShardSyncStrategy = shardSyncStrategyDecider.getShardSyncStrategy();
        assertEquals(String.format("%s Shard sync strategy expected", ShardSyncStrategy.PERIODIC.toString()),
                ShardSyncStrategy.PERIODIC, actualShardSyncStrategy);
    }

    @Test
    public void testPeriodicStrategyResultReturnedWhenActiveLeasesEqualToRequired() throws Exception {
        List<KinesisClientLease> leases = getLeases(MIN_ACTIVE_SHARDS_FOR_PERIODIC_SYNC,
                false /* duplicateLeaseOwner */, true /* activeLeases */);
        when(leaseManager.listLeases()).thenReturn(leases);
        ShardSyncStrategyDecider shardSyncStrategyDecider = new ActiveShardCountBasedShardSyncStrategyDecider(
                leaseManager);
        ShardSyncStrategy actualShardSyncStrategy = shardSyncStrategyDecider.getShardSyncStrategy();
        assertEquals(String.format("%s Shard sync strategy expected", ShardSyncStrategy.PERIODIC.toString()),
                ShardSyncStrategy.PERIODIC, actualShardSyncStrategy);
    }


}
