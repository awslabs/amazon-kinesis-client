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
package com.amazonaws.services.kinesis.leases.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

/**
 * A Lease subclass containing KinesisClientLibrary related fields for checkpoints.
 */
public class KinesisClientLease extends Lease {

    private ExtendedSequenceNumber checkpoint;
    private ExtendedSequenceNumber pendingCheckpoint;
    private Long ownerSwitchesSinceCheckpoint = 0L;
    private Set<String> parentShardIds = new HashSet<String>();

    public KinesisClientLease() {

    }

    public KinesisClientLease(KinesisClientLease other) {
        super(other);
        this.checkpoint = other.getCheckpoint();
        this.pendingCheckpoint = other.getPendingCheckpoint();
        this.ownerSwitchesSinceCheckpoint = other.getOwnerSwitchesSinceCheckpoint();
        this.parentShardIds.addAll(other.getParentShardIds());
    }

    KinesisClientLease(String leaseKey, String leaseOwner, Long leaseCounter, UUID concurrencyToken,
            Long lastCounterIncrementNanos, ExtendedSequenceNumber checkpoint, ExtendedSequenceNumber pendingCheckpoint,
            Long ownerSwitchesSinceCheckpoint, Set<String> parentShardIds) {
        super(leaseKey, leaseOwner, leaseCounter, concurrencyToken, lastCounterIncrementNanos);

        this.checkpoint = checkpoint;
        this.pendingCheckpoint = pendingCheckpoint;
        this.ownerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint;
        this.parentShardIds.addAll(parentShardIds);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends Lease> void update(T other) {
        super.update(other);
        if (!(other instanceof KinesisClientLease)) {
            throw new IllegalArgumentException("Must pass KinesisClientLease object to KinesisClientLease.update(Lease)");
        }
        KinesisClientLease casted = (KinesisClientLease) other;

        setOwnerSwitchesSinceCheckpoint(casted.ownerSwitchesSinceCheckpoint);
        setCheckpoint(casted.checkpoint);
        setPendingCheckpoint(casted.pendingCheckpoint);
        setParentShardIds(casted.parentShardIds);
    }

    /**
     * @return most recently application-supplied checkpoint value. During fail over, the new worker will pick up after
     *         the old worker's last checkpoint.
     */
    public ExtendedSequenceNumber getCheckpoint() {
        return checkpoint;
    }

    /**
     * @return pending checkpoint, possibly null.
     */
    public ExtendedSequenceNumber getPendingCheckpoint() {
        return pendingCheckpoint;
    }

    /**
     * @return count of distinct lease holders between checkpoints.
     */
    public Long getOwnerSwitchesSinceCheckpoint() {
        return ownerSwitchesSinceCheckpoint;
    }

    /**
     * @return shardIds that parent this lease. Used for resharding.
     */
    public Set<String> getParentShardIds() {
        return new HashSet<String>(parentShardIds);
    }

    /**
     * Sets checkpoint.
     * 
     * @param checkpoint may not be null
     */
    public void setCheckpoint(ExtendedSequenceNumber checkpoint) {
        verifyNotNull(checkpoint, "Checkpoint should not be null");

        this.checkpoint = checkpoint;
    }

    /**
     * Sets pending checkpoint.
     *
     * @param pendingCheckpoint can be null
     */
    public void setPendingCheckpoint(ExtendedSequenceNumber pendingCheckpoint) {
        this.pendingCheckpoint = pendingCheckpoint;
    }

    /**
     * Sets ownerSwitchesSinceCheckpoint.
     * 
     * @param ownerSwitchesSinceCheckpoint may not be null
     */
    public void setOwnerSwitchesSinceCheckpoint(Long ownerSwitchesSinceCheckpoint) {
        verifyNotNull(ownerSwitchesSinceCheckpoint, "ownerSwitchesSinceCheckpoint should not be null");

        this.ownerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint;
    }

    /**
     * Sets parentShardIds.
     * 
     * @param parentShardIds may not be null
     */
    public void setParentShardIds(Collection<String> parentShardIds) {
        verifyNotNull(parentShardIds, "parentShardIds should not be null");

        this.parentShardIds.clear();
        this.parentShardIds.addAll(parentShardIds);
    }
    
    private void verifyNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((checkpoint == null) ? 0 : checkpoint.hashCode());
        result = pendingCheckpoint == null ? result : prime * result + pendingCheckpoint.hashCode();
        result =
                prime * result + ((ownerSwitchesSinceCheckpoint == null) ? 0 : ownerSwitchesSinceCheckpoint.hashCode());
        result = prime * result + ((parentShardIds == null) ? 0 : parentShardIds.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        KinesisClientLease other = (KinesisClientLease) obj;
        if (checkpoint == null) {
            if (other.checkpoint != null)
                return false;
        } else if (!checkpoint.equals(other.checkpoint))
            return false;
        if (pendingCheckpoint == null) {
            if (other.pendingCheckpoint != null)
                return false;
        } else if (!pendingCheckpoint.equals(other.pendingCheckpoint))
            return false;
        if (ownerSwitchesSinceCheckpoint == null) {
            if (other.ownerSwitchesSinceCheckpoint != null)
                return false;
        } else if (!ownerSwitchesSinceCheckpoint.equals(other.ownerSwitchesSinceCheckpoint))
            return false;
        if (parentShardIds == null) {
            if (other.parentShardIds != null)
                return false;
        } else if (!parentShardIds.equals(other.parentShardIds))
            return false;
        return true;
    }

    /**
     * Returns a deep copy of this object. Type-unsafe - there aren't good mechanisms for copy-constructing generics.
     * 
     * @return A deep copy of this object.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends Lease> T copy() {
        return (T) new KinesisClientLease(this);
    }

}
