/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Collections2;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * This class contains data pertaining to a Lease. Distributed systems may use leases to partition work across a
 * fleet of workers. Each unit of work (identified by a leaseKey) has a corresponding Lease. Every worker will contend
 * for all leases - only one worker will successfully take each one. The worker should hold the lease until it is ready to stop
 * processing the corresponding unit of work, or until it fails. When the worker stops holding the lease, another worker will
 * take and hold the lease.
 */
@NoArgsConstructor
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode(exclude = {"concurrencyToken", "lastCounterIncrementNanos"})
@ToString
public class Lease {
    /*
     * See javadoc for System.nanoTime - summary:
     * 
     * Sometimes System.nanoTime's return values will wrap due to overflow. When they do, the difference between two
     * values will be very large. We will consider leases to be expired if they are more than a year old.
     */
    private static final long MAX_ABS_AGE_NANOS = TimeUnit.DAYS.toNanos(365);

    /**
     * @return leaseKey - identifies the unit of work associated with this lease.
     */
    private String leaseKey;
    /**
     * @return current owner of the lease, may be null.
     */
    private String leaseOwner;
    /**
     * @return leaseCounter is incremented periodically by the holder of the lease. Used for optimistic locking.
     */
    private Long leaseCounter = 0L;

    /*
     * This field is used to prevent updates to leases that we have lost and re-acquired. It is deliberately not
     * persisted in DynamoDB and excluded from hashCode and equals.
     */
    private UUID concurrencyToken;

    /*
     * This field is used by LeaseRenewer and LeaseTaker to track the last time a lease counter was incremented. It is
     * deliberately not persisted in DynamoDB and excluded from hashCode and equals.
     */
    private Long lastCounterIncrementNanos;
    /**
     * @return most recently application-supplied checkpoint value. During fail over, the new worker will pick up after
     *         the old worker's last checkpoint.
     */
    private ExtendedSequenceNumber checkpoint;
    /**
     * @return pending checkpoint, possibly null.
     */
    private ExtendedSequenceNumber pendingCheckpoint;
    /**
     * @return count of distinct lease holders between checkpoints.
     */
    private Long ownerSwitchesSinceCheckpoint = 0L;
    private Set<String> parentShardIds = new HashSet<>();

    /**
     * Copy constructor, used by clone().
     * 
     * @param lease lease to copy
     */
    protected Lease(Lease lease) {
        this(lease.leaseKey(), lease.leaseOwner(), lease.leaseCounter(), lease.concurrencyToken(),
                lease.lastCounterIncrementNanos(), lease.checkpoint(), lease.pendingCheckpoint(),
                lease.ownerSwitchesSinceCheckpoint(), lease.parentShardIds());
    }

    public Lease(final String leaseKey, final String leaseOwner, final Long leaseCounter,
                    final UUID concurrencyToken, final Long lastCounterIncrementNanos,
                    final ExtendedSequenceNumber checkpoint, final ExtendedSequenceNumber pendingCheckpoint,
                    final Long ownerSwitchesSinceCheckpoint, final Set<String> parentShardIds) {
        this.leaseKey = leaseKey;
        this.leaseOwner = leaseOwner;
        this.leaseCounter = leaseCounter;
        this.concurrencyToken = concurrencyToken;
        this.lastCounterIncrementNanos = lastCounterIncrementNanos;
        this.checkpoint = checkpoint;
        this.pendingCheckpoint = pendingCheckpoint;
        this.ownerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint;
        if (parentShardIds != null) {
            this.parentShardIds.addAll(parentShardIds);
        }
    }

    /**
     * @return shardIds that parent this lease. Used for resharding.
     */
    public Set<String> parentShardIds() {
        return new HashSet<>(parentShardIds);
    }

    /**
     * Updates this Lease's mutable, application-specific fields based on the passed-in lease object. Does not update
     * fields that are internal to the leasing library (leaseKey, leaseOwner, leaseCounter).
     * 
     * @param lease
     */
    public void update(final Lease lease) {
        ownerSwitchesSinceCheckpoint(lease.ownerSwitchesSinceCheckpoint());
        checkpoint(lease.checkpoint);
        pendingCheckpoint(lease.pendingCheckpoint);
        parentShardIds(lease.parentShardIds);
    }

    /**
     * @param leaseDurationNanos duration of lease in nanoseconds
     * @param asOfNanos time in nanoseconds to check expiration as-of
     * @return true if lease is expired as-of given time, false otherwise
     */
    public boolean isExpired(long leaseDurationNanos, long asOfNanos) {
        if (lastCounterIncrementNanos == null) {
            return true;
        }

        long age = asOfNanos - lastCounterIncrementNanos;
        // see comment on MAX_ABS_AGE_NANOS
        if (Math.abs(age) > MAX_ABS_AGE_NANOS) {
            return true;
        } else {
            return age > leaseDurationNanos;
        }
    }

    /**
     * Sets lastCounterIncrementNanos
     * 
     * @param lastCounterIncrementNanos last renewal in nanoseconds since the epoch
     */
    public void lastCounterIncrementNanos(Long lastCounterIncrementNanos) {
        this.lastCounterIncrementNanos = lastCounterIncrementNanos;
    }

    /**
     * Sets concurrencyToken.
     * 
     * @param concurrencyToken may not be null
     */
    public void concurrencyToken(@NonNull final UUID concurrencyToken) {
        this.concurrencyToken = concurrencyToken;
    }

    /**
     * Sets leaseKey. LeaseKey is immutable once set.
     * 
     * @param leaseKey may not be null.
     */
    public void leaseKey(@NonNull final String leaseKey) {
        if (this.leaseKey != null) {
            throw new IllegalArgumentException("LeaseKey is immutable once set");
        }
        this.leaseKey = leaseKey;
    }

    /**
     * Sets leaseCounter.
     * 
     * @param leaseCounter may not be null
     */
    public void leaseCounter(@NonNull final Long leaseCounter) {
        this.leaseCounter = leaseCounter;
    }

    /**
     * Sets checkpoint.
     *
     * @param checkpoint may not be null
     */
    public void checkpoint(@NonNull final ExtendedSequenceNumber checkpoint) {
        this.checkpoint = checkpoint;
    }

    /**
     * Sets pending checkpoint.
     *
     * @param pendingCheckpoint can be null
     */
    public void pendingCheckpoint(ExtendedSequenceNumber pendingCheckpoint) {
        this.pendingCheckpoint = pendingCheckpoint;
    }

    /**
     * Sets ownerSwitchesSinceCheckpoint.
     *
     * @param ownerSwitchesSinceCheckpoint may not be null
     */
    public void ownerSwitchesSinceCheckpoint(@NonNull final Long ownerSwitchesSinceCheckpoint) {
        this.ownerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint;
    }

    /**
     * Sets parentShardIds.
     *
     * @param parentShardIds may not be null
     */
    public void parentShardIds(@NonNull final Collection<String> parentShardIds) {
        this.parentShardIds.clear();
        this.parentShardIds.addAll(parentShardIds);
    }

    /**
     * Sets leaseOwner.
     * 
     * @param leaseOwner may be null.
     */
    public void leaseOwner(String leaseOwner) {
        this.leaseOwner = leaseOwner;
    }

    /**
     * Returns a deep copy of this object. Type-unsafe - there aren't good mechanisms for copy-constructing generics.
     * 
     * @return A deep copy of this object.
     */
    public Lease copy() {
        return new Lease(this);
    }
}
