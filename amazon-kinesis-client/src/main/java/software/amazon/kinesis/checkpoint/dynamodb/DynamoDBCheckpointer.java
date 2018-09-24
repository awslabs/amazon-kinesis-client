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

package software.amazon.kinesis.checkpoint.dynamodb;

import java.util.Objects;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 *
 */
@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class DynamoDBCheckpointer implements Checkpointer {
    @NonNull
    private final LeaseCoordinator leaseCoordinator;
    @NonNull
    private final LeaseRefresher leaseRefresher;

    private String operation;

    @Override
    public void setCheckpoint(final String shardId, final ExtendedSequenceNumber checkpointValue,
            final String concurrencyToken) throws KinesisClientLibException {
        try {
            boolean wasSuccessful = setCheckpoint(shardId, checkpointValue, UUID.fromString(concurrencyToken));
            if (!wasSuccessful) {
                throw new ShutdownException("Can't update checkpoint - instance doesn't hold the lease for this shard");
            }
        } catch (ProvisionedThroughputException e) {
            throw new ThrottlingException("Got throttled while updating checkpoint.", e);
        } catch (InvalidStateException e) {
            String message = "Unable to save checkpoint for shardId " + shardId;
            log.error(message, e);
            throw new software.amazon.kinesis.exceptions.InvalidStateException(message, e);
        } catch (DependencyException e) {
            throw new KinesisClientLibDependencyException("Unable to save checkpoint for shardId " + shardId, e);
        }
    }

    @Override
    public ExtendedSequenceNumber getCheckpoint(final String shardId) throws KinesisClientLibException {
        try {
            return leaseRefresher.getLease(shardId).checkpoint();
        } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
            String message = "Unable to fetch checkpoint for shardId " + shardId;
            log.error(message, e);
            throw new KinesisClientLibIOException(message, e);
        }
    }

    @Override
    public Checkpoint getCheckpointObject(final String shardId) throws KinesisClientLibException {
        try {
            Lease lease = leaseRefresher.getLease(shardId);
            log.debug("[{}] Retrieved lease => {}", shardId, lease);
            return new Checkpoint(lease.checkpoint(), lease.pendingCheckpoint());
        } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
            String message = "Unable to fetch checkpoint for shardId " + shardId;
            log.error(message, e);
            throw new KinesisClientLibIOException(message, e);
        }
    }

    @Override
    public void prepareCheckpoint(final String shardId, final ExtendedSequenceNumber pendingCheckpoint,
            final String concurrencyToken) throws KinesisClientLibException {
        try {
            boolean wasSuccessful =
                    prepareCheckpoint(shardId, pendingCheckpoint, UUID.fromString(concurrencyToken));
            if (!wasSuccessful) {
                throw new ShutdownException(
                        "Can't prepare checkpoint - instance doesn't hold the lease for this shard");
            }
        } catch (ProvisionedThroughputException e) {
            throw new ThrottlingException("Got throttled while preparing checkpoint.", e);
        } catch (InvalidStateException e) {
            String message = "Unable to prepare checkpoint for shardId " + shardId;
            log.error(message, e);
            throw new software.amazon.kinesis.exceptions.InvalidStateException(message, e);
        } catch (DependencyException e) {
            throw new KinesisClientLibDependencyException("Unable to prepare checkpoint for shardId " + shardId, e);
        }
    }

    @VisibleForTesting
    public boolean setCheckpoint(String shardId, ExtendedSequenceNumber checkpoint, UUID concurrencyToken)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Lease lease = leaseCoordinator.getCurrentlyHeldLease(shardId);
        if (lease == null) {
            log.info("Worker {} could not update checkpoint for shard {} because it does not hold the lease",
                    leaseCoordinator.workerIdentifier(), shardId);
            return false;
        }

        lease.checkpoint(checkpoint);
        lease.pendingCheckpoint(null);
        lease.ownerSwitchesSinceCheckpoint(0L);

        return leaseCoordinator.updateLease(lease, concurrencyToken, operation, shardId);
    }

    boolean prepareCheckpoint(String shardId, ExtendedSequenceNumber pendingCheckpoint, UUID concurrencyToken)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Lease lease = leaseCoordinator.getCurrentlyHeldLease(shardId);
        if (lease == null) {
            log.info("Worker {} could not prepare checkpoint for shard {} because it does not hold the lease",
                    leaseCoordinator.workerIdentifier(), shardId);
            return false;
        }

        lease.pendingCheckpoint(Objects.requireNonNull(pendingCheckpoint, "pendingCheckpoint should not be null"));
        return leaseCoordinator.updateLease(lease, concurrencyToken, operation, shardId);
    }

    @Override
    public void operation(@NonNull final String operation) {
        this.operation = operation;
    }

    @Override
    public String operation() {
        return operation;
    }
}
