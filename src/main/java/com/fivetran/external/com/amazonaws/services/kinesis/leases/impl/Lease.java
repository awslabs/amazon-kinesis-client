/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.fivetran.external.com.amazonaws.services.kinesis.leases.impl;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.amazonaws.util.json.Jackson;

/**
 * This class contains data pertaining to a Lease. Distributed systems may use leases to partition work across a
 * fleet of workers. Each unit of work (identified by a leaseKey) has a corresponding Lease. Every worker will contend
 * for all leases - only one worker will successfully take each one. The worker should hold the lease until it is ready to stop
 * processing the corresponding unit of work, or until it fails. When the worker stops holding the lease, another worker will
 * take and hold the lease.
 */
public class Lease {
    /*
     * See javadoc for System.nanoTime - summary:
     * 
     * Sometimes System.nanoTime's return values will wrap due to overflow. When they do, the difference between two
     * values will be very large. We will consider leases to be expired if they are more than a year old.
     */
    private static final long MAX_ABS_AGE_NANOS = TimeUnit.DAYS.toNanos(365);

    private String leaseKey;
    private String leaseOwner;
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
     * Constructor.
     */
    public Lease() {
    }

    /**
     * Copy constructor, used by clone().
     * 
     * @param lease lease to copy
     */
    protected Lease(Lease lease) {
        this(lease.getLeaseKey(), lease.getLeaseOwner(), lease.getLeaseCounter(), lease.getConcurrencyToken(),
                lease.getLastCounterIncrementNanos());
    }

    protected Lease(String leaseKey, String leaseOwner, Long leaseCounter, UUID concurrencyToken,
            Long lastCounterIncrementNanos) {
        this.leaseKey = leaseKey;
        this.leaseOwner = leaseOwner;
        this.leaseCounter = leaseCounter;
        this.concurrencyToken = concurrencyToken;
        this.lastCounterIncrementNanos = lastCounterIncrementNanos;
    }

    /**
     * Updates this Lease's mutable, application-specific fields based on the passed-in lease object. Does not update
     * fields that are internal to the leasing library (leaseKey, leaseOwner, leaseCounter).
     * 
     * @param other
     */
    public <T extends Lease> void update(T other) {
        // The default implementation (no application-specific fields) has nothing to do.
    }

    /**
     * @return leaseKey - identifies the unit of work associated with this lease.
     */
    public String getLeaseKey() {
        return leaseKey;
    }

    /**
     * @return leaseCounter is incremented periodically by the holder of the lease. Used for optimistic locking.
     */
    public Long getLeaseCounter() {
        return leaseCounter;
    }

    /**
     * @return current owner of the lease, may be null.
     */
    public String getLeaseOwner() {
        return leaseOwner;
    }

    /**
     * @return concurrency token
     */
    public UUID getConcurrencyToken() {
        return concurrencyToken;
    }

    /**
     * @return last update in nanoseconds since the epoch
     */
    public Long getLastCounterIncrementNanos() {
        return lastCounterIncrementNanos;
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
    public void setLastCounterIncrementNanos(Long lastCounterIncrementNanos) {
        this.lastCounterIncrementNanos = lastCounterIncrementNanos;
    }

    /**
     * Sets concurrencyToken.
     * 
     * @param concurrencyToken may not be null
     */
    public void setConcurrencyToken(UUID concurrencyToken) {
        verifyNotNull(concurrencyToken, "concurencyToken cannot be null");
        this.concurrencyToken = concurrencyToken;
    }

    /**
     * Sets leaseKey. LeaseKey is immutable once set.
     * 
     * @param leaseKey may not be null.
     */
    public void setLeaseKey(String leaseKey) {
        if (this.leaseKey != null) {
            throw new IllegalArgumentException("LeaseKey is immutable once set");
        }
        verifyNotNull(leaseKey, "LeaseKey cannot be set to null");

        this.leaseKey = leaseKey;
    }

    /**
     * Sets leaseCounter.
     * 
     * @param leaseCounter may not be null
     */
    public void setLeaseCounter(Long leaseCounter) {
        verifyNotNull(leaseCounter, "leaseCounter must not be null");

        this.leaseCounter = leaseCounter;
    }

    /**
     * Sets leaseOwner.
     * 
     * @param leaseOwner may be null.
     */
    public void setLeaseOwner(String leaseOwner) {
        this.leaseOwner = leaseOwner;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((leaseCounter == null) ? 0 : leaseCounter.hashCode());
        result = prime * result + ((leaseOwner == null) ? 0 : leaseOwner.hashCode());
        result = prime * result + ((leaseKey == null) ? 0 : leaseKey.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Lease other = (Lease) obj;
        if (leaseCounter == null) {
            if (other.leaseCounter != null)
                return false;
        } else if (!leaseCounter.equals(other.leaseCounter))
            return false;
        if (leaseOwner == null) {
            if (other.leaseOwner != null)
                return false;
        } else if (!leaseOwner.equals(other.leaseOwner))
            return false;
        if (leaseKey == null) {
            if (other.leaseKey != null)
                return false;
        } else if (!leaseKey.equals(other.leaseKey))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return Jackson.toJsonPrettyString(this);
    }

    /**
     * Returns a deep copy of this object. Type-unsafe - there aren't good mechanisms for copy-constructing generics.
     * 
     * @return A deep copy of this object.
     */
    @SuppressWarnings("unchecked")
    public <T extends Lease> T copy() {
        return (T) new Lease(this);
    }
    
    private void verifyNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

}
