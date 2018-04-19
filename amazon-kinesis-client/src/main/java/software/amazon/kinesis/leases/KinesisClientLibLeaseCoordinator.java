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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.IMetricsFactory;

/**
 * This class is used to coordinate/manage leases owned by this worker process and to get/set checkpoints.
 */
@Slf4j
public class KinesisClientLibLeaseCoordinator extends LeaseCoordinator<KinesisClientLease> {
    private static final long DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY = 10L;
    private static final long DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10L;

    @Getter
    @Accessors(fluent = true)
    private final LeaseManager<KinesisClientLease> leaseManager;

    private long initialLeaseTableReadCapacity = DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY;
    private long initialLeaseTableWriteCapacity = DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY;

    public KinesisClientLibLeaseCoordinator(final LeaseManager<KinesisClientLease> leaseManager,
                                            final String workerIdentifier,
                                            final long leaseDurationMillis,
                                            final long epsilonMillis,
                                            final int maxLeasesForWorker,
                                            final int maxLeasesToStealAtOneTime,
                                            final int maxLeaseRenewerThreadCount,
                                            final IMetricsFactory metricsFactory) {
        super(leaseManager, workerIdentifier, leaseDurationMillis, epsilonMillis, maxLeasesForWorker,
                maxLeasesToStealAtOneTime, maxLeaseRenewerThreadCount, metricsFactory);
        this.leaseManager = leaseManager;
    }

    /**
     * @param readCapacity The DynamoDB table used for tracking leases will be provisioned with the specified initial
     *        read capacity
     * @return KinesisClientLibLeaseCoordinator
     */
    public KinesisClientLibLeaseCoordinator initialLeaseTableReadCapacity(long readCapacity) {
        if (readCapacity <= 0) {
            throw new IllegalArgumentException("readCapacity should be >= 1");
        }
        this.initialLeaseTableReadCapacity = readCapacity;
        return this;
    }

    /**
     * @param writeCapacity The DynamoDB table used for tracking leases will be provisioned with the specified initial
     *        write capacity
     * @return KinesisClientLibLeaseCoordinator
     */
    public KinesisClientLibLeaseCoordinator initialLeaseTableWriteCapacity(long writeCapacity) {
        if (writeCapacity <= 0) {
            throw new IllegalArgumentException("writeCapacity should be >= 1");
        }
        this.initialLeaseTableWriteCapacity = writeCapacity;
        return this;
    }

    /**
     * @return Current shard/lease assignments
     */
    public List<ShardInfo> getCurrentAssignments() {
        Collection<KinesisClientLease> leases = getAssignments();
        return convertLeasesToAssignments(leases);

    }

    public static List<ShardInfo> convertLeasesToAssignments(Collection<KinesisClientLease> leases) {
        if (leases == null || leases.isEmpty()) {
            return Collections.emptyList();
        }
        List<ShardInfo> assignments = new ArrayList<>(leases.size());
        for (KinesisClientLease lease : leases) {
            assignments.add(convertLeaseToAssignment(lease));
        }

        return assignments;
    }

    public static ShardInfo convertLeaseToAssignment(KinesisClientLease lease) {
        Set<String> parentShardIds = lease.getParentShardIds();
        return new ShardInfo(lease.getLeaseKey(), lease.getConcurrencyToken().toString(), parentShardIds,
                lease.getCheckpoint());
    }

    /**
     * Initialize the lease coordinator (create the lease table if needed).
     * @throws DependencyException
     * @throws ProvisionedThroughputException
     */
    public void initialize() throws ProvisionedThroughputException, DependencyException, IllegalStateException {
        final boolean newTableCreated =
                leaseManager.createLeaseTableIfNotExists(initialLeaseTableReadCapacity, initialLeaseTableWriteCapacity);
        if (newTableCreated) {
            log.info("Created new lease table for coordinator with initial read capacity of {} and write capacity of {}.",
                initialLeaseTableReadCapacity, initialLeaseTableWriteCapacity);
        }
        // Need to wait for table in active state.
        final long secondsBetweenPolls = 10L;
        final long timeoutSeconds = 600L;
        final boolean isTableActive = leaseManager.waitUntilLeaseTableExists(secondsBetweenPolls, timeoutSeconds);
        if (!isTableActive) {
            throw new DependencyException(new IllegalStateException("Creating table timeout"));
        }
    }

    /**
     * Package access for testing.
     * 
     * @throws DependencyException
     * @throws InvalidStateException
     */
    public void runLeaseTaker() throws DependencyException, InvalidStateException {
        super.runTaker();
    }

    /**
     * Package access for testing.
     * 
     * @throws DependencyException
     * @throws InvalidStateException
     */
    public void runLeaseRenewer() throws DependencyException, InvalidStateException {
        super.runRenewer();
    }

}
