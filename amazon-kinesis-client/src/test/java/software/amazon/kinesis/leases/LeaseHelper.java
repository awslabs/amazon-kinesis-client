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

import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.util.Collection;
import java.util.Collections;

public class LeaseHelper {

    public static Lease createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds) {
        return createLease(leaseKey, leaseOwner, parentShardIds, Collections.emptySet(), ExtendedSequenceNumber.LATEST);
    }

    public static Lease createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds, Collection<String> childShardIds) {
        return createLease(leaseKey, leaseOwner, parentShardIds, childShardIds, ExtendedSequenceNumber.LATEST);
    }

    public static Lease createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds,
                              Collection<String> childShardIds, ExtendedSequenceNumber extendedSequenceNumber) {
        Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.leaseOwner(leaseOwner);
        lease.parentShardIds(parentShardIds);
        lease.childShardIds(childShardIds);
        lease.checkpoint(extendedSequenceNumber);

        return lease;
    }
}
