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
package com.amazonaws.services.kinesis.leases.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

public class KinesisClientLeaseBuilder {
    private String leaseKey;
    private String leaseOwner;
    private Long leaseCounter = 0L;
    private UUID concurrencyToken;
    private Long lastCounterIncrementNanos;
    private ExtendedSequenceNumber checkpoint;
    private ExtendedSequenceNumber pendingCheckpoint;
    private Long ownerSwitchesSinceCheckpoint = 0L;
    private Set<String> parentShardIds  = new HashSet<>();
    private Set<String> childShardIds = new HashSet<>();
    private HashKeyRangeForLease hashKeyRangeForLease;

    public KinesisClientLeaseBuilder withLeaseKey(String leaseKey) {
        this.leaseKey = leaseKey;
        return this;
    }

    public KinesisClientLeaseBuilder withLeaseOwner(String leaseOwner) {
        this.leaseOwner = leaseOwner;
        return this;
    }

    public KinesisClientLeaseBuilder withLeaseCounter(Long leaseCounter) {
        this.leaseCounter = leaseCounter;
        return this;
    }

    public KinesisClientLeaseBuilder withConcurrencyToken(UUID concurrencyToken) {
        this.concurrencyToken = concurrencyToken;
        return this;
    }

    public KinesisClientLeaseBuilder withLastCounterIncrementNanos(Long lastCounterIncrementNanos) {
        this.lastCounterIncrementNanos = lastCounterIncrementNanos;
        return this;
    }

    public KinesisClientLeaseBuilder withCheckpoint(ExtendedSequenceNumber checkpoint) {
        this.checkpoint = checkpoint;
        return this;
    }

    public KinesisClientLeaseBuilder withPendingCheckpoint(ExtendedSequenceNumber pendingCheckpoint) {
        this.pendingCheckpoint = pendingCheckpoint;
        return this;
    }

    public KinesisClientLeaseBuilder withOwnerSwitchesSinceCheckpoint(Long ownerSwitchesSinceCheckpoint) {
        this.ownerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint;
        return this;
    }

    public KinesisClientLeaseBuilder withParentShardIds(Set<String> parentShardIds) {
        this.parentShardIds = parentShardIds;
        return this;
    }

    public KinesisClientLeaseBuilder withChildShardIds(Set<String> childShardIds) {
        this.childShardIds = childShardIds;
        return this;
    }

    public KinesisClientLeaseBuilder withHashKeyRange(HashKeyRangeForLease hashKeyRangeForLease) {
        this.hashKeyRangeForLease = hashKeyRangeForLease;
        return this;
    }

    public KinesisClientLease build() {
        return new KinesisClientLease(leaseKey, leaseOwner, leaseCounter, concurrencyToken, lastCounterIncrementNanos,
                checkpoint, pendingCheckpoint, ownerSwitchesSinceCheckpoint, parentShardIds, childShardIds, hashKeyRangeForLease);
    }
}