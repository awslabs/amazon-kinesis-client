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
    private Long ownerSwitchesSinceCheckpoint = 0L;
    private Set<String> parentShardIds  = new HashSet<>();

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

    public KinesisClientLeaseBuilder withOwnerSwitchesSinceCheckpoint(Long ownerSwitchesSinceCheckpoint) {
        this.ownerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint;
        return this;
    }

    public KinesisClientLeaseBuilder withParentShardIds(Set<String> parentShardIds) {
        this.parentShardIds = parentShardIds;
        return this;
    }

    public KinesisClientLease build() {
        return new KinesisClientLease(leaseKey, leaseOwner, leaseCounter, concurrencyToken, lastCounterIncrementNanos,
                checkpoint, ownerSwitchesSinceCheckpoint, parentShardIds);
    }
}