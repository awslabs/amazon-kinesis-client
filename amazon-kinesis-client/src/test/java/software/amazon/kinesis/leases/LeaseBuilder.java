/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.leases;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import lombok.Setter;
import lombok.experimental.Accessors;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@Setter
@Accessors(fluent = true)
public class LeaseBuilder {
    private String leaseKey;
    private String leaseOwner;
    private Long leaseCounter = 0L;
    private UUID concurrencyToken;
    private Long lastCounterIncrementNanos;
    private ExtendedSequenceNumber checkpoint;
    private ExtendedSequenceNumber pendingCheckpoint;
    private Long ownerSwitchesSinceCheckpoint = 0L;
    private Set<String> parentShardIds  = new HashSet<>();

    public Lease build() {
        return new Lease(leaseKey, leaseOwner, leaseCounter, concurrencyToken, lastCounterIncrementNanos,
                checkpoint, pendingCheckpoint, ownerSwitchesSinceCheckpoint, parentShardIds);
    }
}