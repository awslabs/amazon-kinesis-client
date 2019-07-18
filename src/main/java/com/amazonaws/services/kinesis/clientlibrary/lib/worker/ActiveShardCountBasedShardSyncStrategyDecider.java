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

import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ShardSyncStrategyDecider implementation which looks at the active shard count
 * to decide the strategy for shard sync
 *
 */
class ActiveShardCountBasedShardSyncStrategyDecider implements ShardSyncStrategyDecider {

    private static final Object LOCK = new Object();
    private static final Log LOG = LogFactory.getLog(ActiveShardCountBasedShardSyncStrategyDecider.class);
    private ILeaseManager<KinesisClientLease> leaseManager;
    private volatile List<KinesisClientLease> leases;

    ActiveShardCountBasedShardSyncStrategyDecider(ILeaseManager<KinesisClientLease> leaseManager) {
        this.leaseManager = leaseManager;
    }

    static final int MIN_ACTIVE_SHARDS_FOR_PERIODIC_SYNC = 3000;

    /*
     * Gets the active shards count and decides the shard sync strategy based on the threshold
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.
     * ShardSyncStrategyDecider#getShardSyncStrategy()
     */
    @Override
    public ShardSyncStrategy getShardSyncStrategy()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ShardSyncStrategy shardSyncStrategy = null;
        int activeShardleasesCount = getActiveShardleasesCount();
        if (activeShardleasesCount != 0) {
            shardSyncStrategy = activeShardleasesCount >= MIN_ACTIVE_SHARDS_FOR_PERIODIC_SYNC ? ShardSyncStrategy.PERIODIC
                                        : ShardSyncStrategy.SHARD_END;
        }
        LOG.debug(String.format("ActiveShardLeasesCount: %d, ShardSyncStrategy: %s", activeShardleasesCount, shardSyncStrategy.name()));
        return shardSyncStrategy;
    }

    /**
     * Gets the leases and filters out the SHARD_END leases to get the count of
     * active leases
     *
     * @return Active leases count
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    private int getActiveShardleasesCount()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        // double checked locking to ensure only a single thread populates the
        // leases as all threads will get roughly the same view of the leases at
        // startup. This keeps a check on IOPS utilisation of Leases table. Also
        // once populated, the strategy will never move back from Periodic to
        // Shard sync as the number of shards never reduce. For the other case
        // of increasing shards, its a fair assumption that there would be at
        // least one deployment and the workers would initialize again before
        // that happens
        if(leases == null) {
            synchronized (LOCK) {
                if (leases == null) {
                    getLeases();
                }
            }
        }
        return leases == null ? 0
                       : (int) leases.stream().filter(lease -> lease.getCheckpoint() != null && lease.getCheckpoint() != ExtendedSequenceNumber.SHARD_END)
                                     .count();
    }

    private void getLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        leases = leaseManager.listLeases();
    }

}
