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

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

import java.util.Collection;
import java.util.Collections;

public class LeaseHelper {

    public static KinesisClientLease createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds) {
        return createLease(leaseKey, leaseOwner, parentShardIds, Collections.emptySet(), ExtendedSequenceNumber.LATEST);
    }

    public static KinesisClientLease  createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds, Collection<String> childShardIds) {
        return createLease(leaseKey, leaseOwner, parentShardIds, childShardIds, ExtendedSequenceNumber.LATEST);
    }

    public static KinesisClientLease  createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds,
                                    Collection<String> childShardIds, ExtendedSequenceNumber extendedSequenceNumber) {
        KinesisClientLease  lease = new KinesisClientLease ();
        lease.setLeaseKey(leaseKey);
        lease.setLeaseOwner(leaseOwner);
        lease.setParentShardIds(parentShardIds);
        lease.setChildShardIds(childShardIds);
        lease.setCheckpoint(extendedSequenceNumber);

        return lease;
    }
}