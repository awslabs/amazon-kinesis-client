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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Supports basic CRUD operations for Leases.
 */
public interface LeaseRefresher {

    /**
     * Creates the table that will store leases. Succeeds if table already exists.
     * Deprecated. Use {@link #createLeaseTableIfNotExists()}.
     *
     * @param readCapacity
     * @param writeCapacity
     *
     * @return true if we created a new table (table didn't exist before)
     *
     * @throws ProvisionedThroughputException if we cannot create the lease table due to per-AWS-account capacity
     *         restrictions.
     * @throws DependencyException if DynamoDB createTable fails in an unexpected way
     */
    @Deprecated
    boolean createLeaseTableIfNotExists(Long readCapacity, Long writeCapacity)
            throws ProvisionedThroughputException, DependencyException;

    /**
     * Creates the table that will store leases. Table is now created in PayPerRequest billing mode by default.
     * Succeeds if table already exists.
     *
     * @return true if we created a new table (table didn't exist before)
     *
     * @throws ProvisionedThroughputException if we cannot create the lease table due to per-AWS-account capacity
     *         restrictions.
     * @throws DependencyException if DynamoDB createTable fails in an unexpected way
     */
    boolean createLeaseTableIfNotExists() throws ProvisionedThroughputException, DependencyException;

    /**
     * @return true if the lease table already exists.
     *
     * @throws DependencyException if DynamoDB describeTable fails in an unexpected way
     */
    boolean leaseTableExists() throws DependencyException;

    /**
     * Blocks until the lease table exists by polling leaseTableExists.
     *
     * @param secondsBetweenPolls time to wait between polls in seconds
     * @param timeoutSeconds total time to wait in seconds
     *
     * @return true if table exists, false if timeout was reached
     *
     * @throws DependencyException if DynamoDB describeTable fails in an unexpected way
     */
    boolean waitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds) throws DependencyException;

    /**
     * Creates the LeaseOwnerToLeaseKey index on the lease table if it doesn't exist and returns the status of index.
     *
     * @return indexStatus status of the index.
     * @throws DependencyException if storage's describe API fails in an unexpected way
     */
    default String createLeaseOwnerToLeaseKeyIndexIfNotExists() throws DependencyException {
        return null;
    }

    /**
     * Blocks until the index exists by polling storage till either the index is ACTIVE or else timeout has
     * happened.
     *
     * @param secondsBetweenPolls time to wait between polls in seconds
     * @param timeoutSeconds total time to wait in seconds
     *
     * @return true if index on the table exists and is ACTIVE, false if timeout was reached
     */
    default boolean waitUntilLeaseOwnerToLeaseKeyIndexExists(
            final long secondsBetweenPolls, final long timeoutSeconds) {
        return false;
    }

    /**
     * Check if leaseOwner GSI is ACTIVE
     * @return  true if index is active, false otherwise
     * @throws DependencyException if storage's describe API fails in an unexpected way
     */
    boolean isLeaseOwnerToLeaseKeyIndexActive() throws DependencyException;

    /**
     * List all leases for a given stream synchronously.
     *
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     *
     * @return list of leases
     */
    List<Lease> listLeasesForStream(StreamIdentifier streamIdentifier)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * List all leases for a given workerIdentifier synchronously.
     * Default implementation calls listLeases() and filters the results.
     *
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     *
     * @return list of leases
     */
    default List<String> listLeaseKeysForWorker(final String workerIdentifier)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return listLeases().stream()
                .filter(lease -> lease.leaseOwner().equals(workerIdentifier))
                .map(Lease::leaseKey)
                .collect(Collectors.toList());
    }

    /**
     * List all objects in table synchronously.
     *
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     *
     * @return list of leases
     */
    List<Lease> listLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * List all leases from the storage parallely and deserialize into Lease objects. Returns the list of leaseKey
     * that failed deserialize separately.
     *
     * @param threadPool        threadpool to use for parallel scan
     * @param parallelismFactor no. of parallel scans
     * @return Pair of List of leases from the storage and List of items failed to deserialize
     * @throws DependencyException            if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException          if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     */
    default Map.Entry<List<Lease>, List<String>> listLeasesParallely(
            final ExecutorService threadPool, final int parallelismFactor)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throw new UnsupportedOperationException("listLeasesParallely is not implemented");
    }

    /**
     * Create a new lease. Conditional on a lease not already existing with this shardId.
     *
     * @param lease the lease to create
     *
     * @return true if lease was created, false if lease already exists
     *
     * @throws DependencyException if DynamoDB put fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB put fails due to lack of capacity
     */
    boolean createLeaseIfNotExists(Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * @param leaseKey Get the lease for this leasekey
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB get fails due to lack of capacity
     * @throws DependencyException if DynamoDB get fails in an unexpected way
     *
     * @return lease for the specified leaseKey, or null if one doesn't exist
     */
    Lease getLease(String leaseKey) throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Renew a lease by incrementing the lease counter. Conditional on the leaseCounter in DynamoDB matching the leaseCounter
     * of the input. Mutates the leaseCounter of the passed-in lease object after updating the record in DynamoDB.
     *
     * @param lease the lease to renew
     *
     * @return true if renewal succeeded, false otherwise
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    boolean renewLease(Lease lease) throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Take a lease for the given owner by incrementing its leaseCounter and setting its owner field. Conditional on
     * the leaseCounter in DynamoDB matching the leaseCounter of the input. Mutates the leaseCounter and owner of the
     * passed-in lease object after updating DynamoDB.
     *
     * @param lease the lease to take
     * @param owner the new owner
     *
     * @return true if lease was successfully taken, false otherwise
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    boolean takeLease(Lease lease, String owner)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Assigns given lease to newOwner owner by incrementing its leaseCounter and setting its owner field. Conditional
     * on the leaseOwner in DynamoDB matching the leaseOwner of the input lease. Mutates the leaseCounter and owner of
     * the passed-in lease object after updating DynamoDB.
     *
     * @param lease the lease to be assigned
     * @param newOwner the new owner
     *
     * @return true if lease was successfully assigned, false otherwise
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    default boolean assignLease(final Lease lease, final String newOwner)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {

        throw new UnsupportedOperationException("assignLease is not implemented");
    }

    /**
     * Initiates a graceful handoff of the given lease to the specified new owner, allowing the current owner
     * to complete its processing before transferring ownership.
     * <p>
     * This method updates the lease with the new owner information but ensures that the current owner
     * is given time to gracefully finish its work (e.g., processing records) before the lease is reassigned.
     * </p>
     *
     * @param lease    the lease to be assigned
     * @param newOwner the new owner
     * @return true if a graceful handoff was successfully initiated
     * @throws InvalidStateException          if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException            if DynamoDB update fails in an unexpected way
     */
    default boolean initiateGracefulLeaseHandoff(final Lease lease, final String newOwner)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {

        throw new UnsupportedOperationException("assignLeaseWithWait is not implemented");
    }

    /**
     * Evict the current owner of lease by setting owner to null. Conditional on the owner in DynamoDB matching the owner of
     * the input. Mutates the lease counter and owner of the passed-in lease object after updating the record in DynamoDB.
     *
     * @param lease the lease to void
     *
     * @return true if eviction succeeded, false otherwise
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    boolean evictLease(Lease lease) throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Delete the given lease from DynamoDB. Does nothing when passed a lease that does not exist in DynamoDB.
     *
     * @param lease the lease to delete
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB delete fails due to lack of capacity
     * @throws DependencyException if DynamoDB delete fails in an unexpected way
     */
    void deleteLease(Lease lease) throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Delete all leases from DynamoDB. Useful for tools/utils and testing.
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan or delete fail due to lack of capacity
     * @throws DependencyException if DynamoDB scan or delete fail in an unexpected way
     */
    void deleteAll() throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Update application-specific fields of the given lease in DynamoDB. Does not update fields managed by the leasing
     * library such as leaseCounter, leaseOwner, or leaseKey. Conditional on the leaseCounter in DynamoDB matching the
     * leaseCounter of the input. Increments the lease counter in DynamoDB so that updates can be contingent on other
     * updates. Mutates the lease counter of the passed-in lease object.
     *
     * @return true if update succeeded, false otherwise
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    boolean updateLease(Lease lease) throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Update application-specific fields of the given lease in DynamoDB. Does not update fields managed by the leasing
     * library such as leaseCounter, leaseOwner, or leaseKey.
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    default void updateLeaseWithMetaInfo(Lease lease, UpdateField updateField)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throw new UnsupportedOperationException("updateLeaseWithNoExpectation is not implemented");
    }

    /**
     * Check (synchronously) if there are any leases in the lease table.
     *
     * @return true if there are no leases in the lease table
     *
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     */
    boolean isLeaseTableEmpty() throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Gets the current checkpoint of the shard. This is useful in the resharding use case
     * where we will wait for the parent shard to complete before starting on the records from a child shard.
     *
     * @param leaseKey Checkpoint of this shard will be returned
     * @return Checkpoint of this shard, or null if the shard record doesn't exist.
     *
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws InvalidStateException if lease table does not exist
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    ExtendedSequenceNumber getCheckpoint(String leaseKey)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException;
}
