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
import java.util.Map;
import java.util.UUID;

import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * ILeaseRenewer objects are used by LeaseCoordinator to renew leases held by the LeaseCoordinator. Each
 * LeaseCoordinator instance corresponds to one worker, and uses exactly one ILeaseRenewer to manage lease renewal for
 * that worker.
 */
public interface LeaseRenewer {
    
    /**
     * Bootstrap initial set of leases from the {@link LeaseRefresher} (e.g. upon process restart, pick up leases we own)
     * @throws DependencyException on unexpected DynamoDB failures
     * @throws InvalidStateException if lease table doesn't exist
     * @throws ProvisionedThroughputException if DynamoDB reads fail due to insufficient capacity
     */
    void initialize() throws DependencyException, InvalidStateException, ProvisionedThroughputException;   

    /**
     * Attempt to renew all currently held leases.
     * 
     * @throws DependencyException on unexpected DynamoDB failures
     * @throws InvalidStateException if lease table does not exist
     */
    void renewLeases() throws DependencyException, InvalidStateException;

    /**
     * @return currently held leases. Key is shardId, value is corresponding Lease object. A lease is currently held if
     *         we successfully renewed it on the last run of renewLeases(). Lease objects returned are deep copies -
     *         their lease counters will not tick.
     */
    Map<String, Lease> getCurrentlyHeldLeases();

    /**
     * @param leaseKey key of the lease to retrieve
     * 
     * @return a deep copy of a currently held lease, or null if we don't hold the lease
     */
    Lease getCurrentlyHeldLease(String leaseKey);

    /**
     * Adds leases to this LeaseRenewer's set of currently held leases. Leases must have lastRenewalNanos set to the
     * last time the lease counter was incremented before being passed to this method.
     * 
     * @param newLeases new leases.
     */
    void addLeasesToRenew(Collection<Lease> newLeases);

    /**
     * Clears this LeaseRenewer's set of currently held leases.
     */
    void clearCurrentlyHeldLeases();

    /**
     * Stops the lease renewer from continunig to maintain the given lease.
     * 
     * @param lease the lease to drop.
     */
    void dropLease(Lease lease);

    /**
     * Update application-specific fields in a currently held lease. Cannot be used to update internal fields such as
     * leaseCounter, leaseOwner, etc. Fails if we do not hold the lease, or if the concurrency token does not match
     * the concurrency token on the internal authoritative copy of the lease (ie, if we lost and re-acquired the lease).
     * 
     * @param lease lease object containing updated data
     * @param concurrencyToken obtained by calling Lease.concurrencyToken for a currently held lease
     * @param operation that performs updateLease
     * @param singleStreamShardId shardId for metrics emission in single stream mode. MultiStream mode will get the
     *                            shardId from the lease object
     * 
     * @return true if update succeeds, false otherwise
     * 
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    boolean updateLease(Lease lease, UUID concurrencyToken, String operation, String singleStreamShardId)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

}
