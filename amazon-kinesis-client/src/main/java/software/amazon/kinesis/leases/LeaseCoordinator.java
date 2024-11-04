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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 *
 */
public interface LeaseCoordinator {
    /**
     * Initialize the lease coordinator (create the lease table if needed).
     * @throws DependencyException
     * @throws ProvisionedThroughputException
     */
    void initialize() throws ProvisionedThroughputException, DependencyException, IllegalStateException;

    /**
     * Start background LeaseHolder and LeaseTaker threads.
     * @param leaseAssignmentModeProvider provider of Lease Assignment mode to determine whether to start components
     *                                    for both V2 and V3 functionality or only V3 functionality
     * @throws ProvisionedThroughputException If we can't talk to DynamoDB due to insufficient capacity.
     * @throws InvalidStateException If the lease table doesn't exist
     * @throws DependencyException If we encountered exception taking to DynamoDB
     */
    void start(final MigrationAdaptiveLeaseAssignmentModeProvider leaseAssignmentModeProvider)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Runs a single iteration of the lease taker - used by integration tests.
     *
     * @throws InvalidStateException
     * @throws DependencyException
     */
    void runLeaseTaker() throws DependencyException, InvalidStateException;

    /**
     * Runs a single iteration of the lease renewer - used by integration tests.
     *
     * @throws InvalidStateException
     * @throws DependencyException
     */
    void runLeaseRenewer() throws DependencyException, InvalidStateException;

    /**
     * @return true if this LeaseCoordinator is running
     */
    boolean isRunning();

    /**
     * @return workerIdentifier
     */
    String workerIdentifier();

    /**
     * @return {@link LeaseRefresher}
     */
    LeaseRefresher leaseRefresher();

    /**
     * @return currently held leases
     */
    Collection<Lease> getAssignments();

    /**
     * @param leaseKey lease key to fetch currently held lease for
     *
     * @return deep copy of currently held Lease for given key, or null if we don't hold the lease for that key
     */
    Lease getCurrentlyHeldLease(String leaseKey);

    /**
     * Updates application-specific lease values in DynamoDB.
     *
     * @param lease lease object containing updated values
     * @param concurrencyToken obtained by calling Lease.concurrencyToken for a currently held lease
     * @param operation that performs updateLease
     * @param singleStreamShardId for metrics emission in single stream mode. MultiStream mode will get the
     *                            shardId from the lease object
     *
     * @return true if update succeeded, false otherwise
     *
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    boolean updateLease(Lease lease, UUID concurrencyToken, String operation, String singleStreamShardId)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Requests the cancellation of the lease taker.
     */
    void stopLeaseTaker();

    /**
     * Requests that renewals for the given lease are stopped.
     *
     * @param lease the lease to stop renewing.
     */
    void dropLease(Lease lease);

    /**
     * Stops background threads and waits for specific amount of time for all background tasks to complete.
     * If tasks are not completed after this time, method will shutdown thread pool forcefully and return.
     */
    void stop();

    /**
     * @return Current shard/lease assignments
     */
    List<ShardInfo> getCurrentAssignments();

    /**
     * Default implementation returns an empty list and concrete implementation is expected to return all leases
     * for the application that are in the lease table. This enables application managing Kcl Scheduler to take care of
     * horizontal scaling for example.
     *
     * @return all leases for the application that are in the lease table
     */
    default List<Lease> allLeases() {
        return Collections.emptyList();
    }

    /**
     * @param writeCapacity The DynamoDB table used for tracking leases will be provisioned with the specified initial
     *        write capacity
     * @return LeaseCoordinator
     */
    DynamoDBLeaseCoordinator initialLeaseTableWriteCapacity(long writeCapacity);

    /**
     * @param readCapacity The DynamoDB table used for tracking leases will be provisioned with the specified initial
     *        read capacity
     * @return LeaseCoordinator
     */
    DynamoDBLeaseCoordinator initialLeaseTableReadCapacity(long readCapacity);

    /**
     * @return instance of {@link LeaseStatsRecorder}
     */
    LeaseStatsRecorder leaseStatsRecorder();
}
