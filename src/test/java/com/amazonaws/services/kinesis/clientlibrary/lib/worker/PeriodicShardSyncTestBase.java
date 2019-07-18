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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;

public class PeriodicShardSyncTestBase {

    private static final String LEASE_KEY = "lease_key";
    private static final String LEASE_OWNER = "lease_owner";

    protected List<KinesisClientLease> getLeases(int count, boolean duplicateLeaseOwner, boolean activeLeases) {
        List<KinesisClientLease> leases = new ArrayList<KinesisClientLease>();
        for (int i=0;i<count;i++) {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setLeaseKey(LEASE_KEY + i);
            lease.setCheckpoint(activeLeases ? ExtendedSequenceNumber.LATEST : ExtendedSequenceNumber.SHARD_END);
            lease.setLeaseCounter(new Random().nextLong());
            lease.setLeaseOwner(LEASE_OWNER + (duplicateLeaseOwner ? "" : i));
            leases.add(lease);
        }
        return leases;
    }
}
