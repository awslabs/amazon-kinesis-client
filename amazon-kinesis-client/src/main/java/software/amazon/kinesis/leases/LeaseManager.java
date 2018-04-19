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

import java.util.List;

import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.leases.Lease;

/**
 * Supports basic CRUD operations for Leases.
 * 
 * @param <T> Lease subclass, possibly Lease itself.
 */
public interface LeaseManager<T extends Lease> {

    /**
     * Creates the table that will store leases. Succeeds if table already exists.
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
    public boolean createLeaseTableIfNotExists(Long readCapacity, Long writeCapacity)
        throws ProvisionedThroughputException, DependencyException;

    /**
     * @return true if the lease table already exists.
     * 
     * @throws DependencyException if DynamoDB describeTable fails in an unexpected way
     */
    public boolean leaseTableExists() throws DependencyException;

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
    public boolean waitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds) throws DependencyException;

    /**
     * List all objects in table synchronously.
     * 
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     * 
     * @return list of leases
     */
    public List<T> listLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException;

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
    public boolean createLeaseIfNotExists(T lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * @param shardId Get the lease for this shardId
     * 
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB get fails due to lack of capacity
     * @throws DependencyException if DynamoDB get fails in an unexpected way
     * 
     * @return lease for the specified shardId, or null if one doesn't exist
     */
    public T getLease(String shardId) throws DependencyException, InvalidStateException, ProvisionedThroughputException;

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
    public boolean renewLease(T lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException;

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
    public boolean takeLease(T lease, String owner)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException;

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
    public boolean evictLease(T lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Delete the given lease from DynamoDB. Does nothing when passed a lease that does not exist in DynamoDB.
     * 
     * @param lease the lease to delete
     * 
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB delete fails due to lack of capacity
     * @throws DependencyException if DynamoDB delete fails in an unexpected way
     */
    public void deleteLease(T lease) throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Delete all leases from DynamoDB. Useful for tools/utils and testing.
     * 
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan or delete fail due to lack of capacity
     * @throws DependencyException if DynamoDB scan or delete fail in an unexpected way
     */
    public void deleteAll() throws DependencyException, InvalidStateException, ProvisionedThroughputException;

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
    public boolean updateLease(T lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Check (synchronously) if there are any leases in the lease table.
     * 
     * @return true if there are no leases in the lease table
     * 
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     */
    public boolean isLeaseTableEmpty() throws DependencyException, InvalidStateException, ProvisionedThroughputException;

}
