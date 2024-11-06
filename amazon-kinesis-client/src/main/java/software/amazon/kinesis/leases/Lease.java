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
package software.amazon.kinesis.leases;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.kinesis.common.HashKeyRangeForLease;
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
@EqualsAndHashCode(
        exclude = {
            "concurrencyToken",
            "lastCounterIncrementNanos",
            "childShardIds",
            "pendingCheckpointState",
            "isMarkedForLeaseSteal",
            "throughputKBps",
            "checkpointOwner",
            "checkpointOwnerTimeoutTimestampMillis",
            "isExpiredOrUnassigned"
        })
@ToString
public class Lease {
    /**
     * See javadoc for System.nanoTime - summary:
     *
     * Sometimes System.nanoTime's return values will wrap due to overflow. When they do, the difference between two
     * values will be very large. We will consider leases to be expired if they are more than a year old.
     */
    private static final long MAX_ABS_AGE_NANOS = TimeUnit.DAYS.toNanos(365);

    /**
     * Identifies the unit of work associated with this lease.
     */
    private String leaseKey;
    /**
     * Current owner of the lease, may be null.
     */
    private String leaseOwner;
    /**
     * LeaseCounter is incremented periodically by the holder of the lease. Used for optimistic locking.
     */
    private Long leaseCounter = 0L;

    /**
     * This field is used to prevent updates to leases that we have lost and re-acquired. It is deliberately not
     * persisted in DynamoDB and excluded from hashCode and equals.
     */
    private UUID concurrencyToken;

    /**
     * This field is used by LeaseRenewer and LeaseTaker to track the last time a lease counter was incremented. It is
     * deliberately not persisted in DynamoDB and excluded from hashCode and equals.
     */
    private Long lastCounterIncrementNanos;
    /**
     * Most recently application-supplied checkpoint value. During fail over, the new worker will pick up after
     *         the old worker's last checkpoint.
     */
    private ExtendedSequenceNumber checkpoint;
    /**
     * Pending checkpoint, possibly null.
     */
    private ExtendedSequenceNumber pendingCheckpoint;

    /**
     * Last pending checkpoint state, possibly null. Deliberately excluded from hashCode and equals.
     */
    private byte[] pendingCheckpointState;

    /**
     * Denotes whether the lease is marked for stealing. Deliberately excluded from hashCode and equals and
     * not persisted in DynamoDB.
     */
    @Setter
    private boolean isMarkedForLeaseSteal;

    /**
     * If true, this indicates that lease is ready to be immediately reassigned.
     */
    @Setter
    private boolean isExpiredOrUnassigned;

    /**
     * Throughput in Kbps for the lease.
     */
    private Double throughputKBps;

    /**
     * Owner of the checkpoint. The attribute is used for graceful shutdowns to indicate the owner that
     * is allowed to write the checkpoint.
     */
    @Setter
    private String checkpointOwner;

    /**
     * This field is used for tracking when the shutdown was requested on the lease so we can expire it. This is
     * deliberately not persisted in DynamoDB because leaseOwner are expected to transfer lease from itself to the
     * next owner during shutdown. If the worker dies before shutdown the lease will just become expired then we can
     * pick it up. If for some reason worker is not able to shut down and continues holding onto the lease
     * this timeout will kick in and force a lease transfer.
     */
    @Setter
    private Long checkpointOwnerTimeoutTimestampMillis;
    /**
     * Count of distinct lease holders between checkpoints.
     */
    private Long ownerSwitchesSinceCheckpoint = 0L;

    private final Set<String> parentShardIds = new HashSet<>();
    private final Set<String> childShardIds = new HashSet<>();
    private HashKeyRangeForLease hashKeyRangeForLease;

    /**
     * Copy constructor, used by clone().
     *
     * @param lease lease to copy
     */
    protected Lease(Lease lease) {
        this(
                lease.leaseKey(),
                lease.leaseOwner(),
                lease.leaseCounter(),
                lease.concurrencyToken(),
                lease.lastCounterIncrementNanos(),
                lease.checkpoint(),
                lease.pendingCheckpoint(),
                lease.ownerSwitchesSinceCheckpoint(),
                lease.parentShardIds(),
                lease.childShardIds(),
                lease.pendingCheckpointState(),
                lease.hashKeyRangeForLease());
    }

    @Deprecated
    public Lease(
            final String leaseKey,
            final String leaseOwner,
            final Long leaseCounter,
            final UUID concurrencyToken,
            final Long lastCounterIncrementNanos,
            final ExtendedSequenceNumber checkpoint,
            final ExtendedSequenceNumber pendingCheckpoint,
            final Long ownerSwitchesSinceCheckpoint,
            final Set<String> parentShardIds) {
        this(
                leaseKey,
                leaseOwner,
                leaseCounter,
                concurrencyToken,
                lastCounterIncrementNanos,
                checkpoint,
                pendingCheckpoint,
                ownerSwitchesSinceCheckpoint,
                parentShardIds,
                new HashSet<>(),
                null,
                null);
    }

    public Lease(
            final String leaseKey,
            final String leaseOwner,
            final Long leaseCounter,
            final UUID concurrencyToken,
            final Long lastCounterIncrementNanos,
            final ExtendedSequenceNumber checkpoint,
            final ExtendedSequenceNumber pendingCheckpoint,
            final Long ownerSwitchesSinceCheckpoint,
            final Set<String> parentShardIds,
            final Set<String> childShardIds,
            final byte[] pendingCheckpointState,
            final HashKeyRangeForLease hashKeyRangeForLease) {
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
        if (childShardIds != null) {
            this.childShardIds.addAll(childShardIds);
        }
        this.hashKeyRangeForLease = hashKeyRangeForLease;
        this.pendingCheckpointState = pendingCheckpointState;
        this.isMarkedForLeaseSteal = false;
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
        pendingCheckpointState(lease.pendingCheckpointState);
        parentShardIds(lease.parentShardIds);
        childShardIds(lease.childShardIds);
    }

    /**
     * @param leaseDurationNanos duration of lease in nanoseconds
     * @param asOfNanos time in nanoseconds to check expiration as-of
     * @return true if lease lease is ready to be taken
     */
    public boolean isAvailable(long leaseDurationNanos, long asOfNanos) {
        return isUnassigned() || isExpired(leaseDurationNanos, asOfNanos);
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
     * @return true if checkpoint owner is set. Indicating a requested shutdown.
     */
    public boolean shutdownRequested() {
        return checkpointOwner != null;
    }

    /**
     * Check whether lease should be blocked on pending checkpoint. We DON'T block if
     * - lease is expired (Expired lease should be assigned right away) OR
     *          ----- at this point we know lease is assigned -----
     * - lease is shardEnd (No more processing possible) OR
     * - lease is NOT requested for shutdown OR
     * - lease shutdown expired
     *
     * @param currentTimeMillis current time in milliseconds
     * @return true if lease is blocked on pending checkpoint
     */
    public boolean blockedOnPendingCheckpoint(long currentTimeMillis) {
        // using ORs and negate
        return !(isExpiredOrUnassigned
                || ExtendedSequenceNumber.SHARD_END.equals(checkpoint)
                || !shutdownRequested()
                // if shutdown requested then checkpointOwnerTimeoutTimestampMillis should present
                || currentTimeMillis - checkpointOwnerTimeoutTimestampMillis >= 0);
    }

    /**
     * Check whether lease is eligible for graceful shutdown. It's eligible if
     * - lease is still assigned (not expired) AND
     * - lease is NOT shardEnd (No more processing possible AND
     * - lease is NOT requested for shutdown
     *
     * @return true if lease is eligible for graceful shutdown
     */
    public boolean isEligibleForGracefulShutdown() {
        return !isExpiredOrUnassigned && !ExtendedSequenceNumber.SHARD_END.equals(checkpoint) && !shutdownRequested();
    }

    /**
     * Need to handle the case during graceful shutdown where leaseOwner isn't the current owner
     *
     * @return the actual owner
     */
    public String actualOwner() {
        return checkpointOwner == null ? leaseOwner : checkpointOwner;
    }

    /**
     * @return true if lease is not currently owned
     */
    private boolean isUnassigned() {
        return leaseOwner == null;
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
     * Sets pending checkpoint state.
     *
     * @param pendingCheckpointState can be null
     */
    public void pendingCheckpointState(byte[] pendingCheckpointState) {
        this.pendingCheckpointState = pendingCheckpointState;
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
     * Sets childShardIds.
     *
     * @param childShardIds may not be null
     */
    public void childShardIds(@NonNull final Collection<String> childShardIds) {
        this.childShardIds.addAll(childShardIds);
    }

    /**
     * Sets throughputKbps.
     *
     * @param throughputKBps may not be null
     */
    public void throughputKBps(double throughputKBps) {
        this.throughputKBps = throughputKBps;
    }

    /**
     * Set the hash range key for this shard.
     * @param hashKeyRangeForLease
     */
    public void hashKeyRange(HashKeyRangeForLease hashKeyRangeForLease) {
        if (this.hashKeyRangeForLease == null) {
            this.hashKeyRangeForLease = hashKeyRangeForLease;
        } else if (!this.hashKeyRangeForLease.equals(hashKeyRangeForLease)) {
            throw new IllegalArgumentException("hashKeyRange is immutable");
        }
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
        final Lease lease = new Lease(this);
        lease.checkpointOwner(this.checkpointOwner);
        return lease;
    }
}
