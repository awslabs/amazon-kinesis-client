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
    public void setCheckpoint(final String leaseKey, final ExtendedSequenceNumber checkpointValue,
            final String concurrencyToken) throws KinesisClientLibException {
        try {
            boolean wasSuccessful = setCheckpoint(leaseKey, checkpointValue, UUID.fromString(concurrencyToken));
            if (!wasSuccessful) {
                throw new ShutdownException("Can't update checkpoint - instance doesn't hold the lease for this shard");
            }
        } catch (ProvisionedThroughputException e) {
            throw new ThrottlingException("Got throttled while updating checkpoint.", e);
        } catch (InvalidStateException e) {
            String message = "Unable to save checkpoint for shardId " + leaseKey;
            log.error(message, e);
            throw new software.amazon.kinesis.exceptions.InvalidStateException(message, e);
        } catch (DependencyException e) {
            throw new KinesisClientLibDependencyException("Unable to save checkpoint for shardId " + leaseKey, e);
        }
    }

    @Override
    public ExtendedSequenceNumber getCheckpoint(final String leaseKey) throws KinesisClientLibException {
        try {
            return leaseRefresher.getLease(leaseKey).checkpoint();
        } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
            String message = "Unable to fetch checkpoint for shardId " + leaseKey;
            log.error(message, e);
            throw new KinesisClientLibIOException(message, e);
        }
    }

    @Override
    public Checkpoint getCheckpointObject(final String leaseKey) throws KinesisClientLibException {
        try {
            Lease lease = leaseRefresher.getLease(leaseKey);
            log.debug("[{}] Retrieved lease => {}", leaseKey, lease);
            return new Checkpoint(lease.checkpoint(), lease.pendingCheckpoint(), lease.pendingCheckpointState());
        } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
            String message = "Unable to fetch checkpoint for shardId " + leaseKey;
            log.error(message, e);
            throw new KinesisClientLibIOException(message, e);
        }
    }

    @Override
    public void prepareCheckpoint(final String leaseKey, final ExtendedSequenceNumber pendingCheckpoint,
            final String concurrencyToken) throws KinesisClientLibException {
        prepareCheckpoint(leaseKey, pendingCheckpoint, concurrencyToken, null);
    }

    @Override
    public void prepareCheckpoint(String leaseKey, ExtendedSequenceNumber pendingCheckpoint, String concurrencyToken,
            byte[] pendingCheckpointState) throws KinesisClientLibException {
        try {
            boolean wasSuccessful =
                    prepareCheckpoint(leaseKey, pendingCheckpoint, UUID.fromString(concurrencyToken), pendingCheckpointState);
            if (!wasSuccessful) {
                throw new ShutdownException(
                        "Can't prepare checkpoint - instance doesn't hold the lease for this shard");
            }
        } catch (ProvisionedThroughputException e) {
            throw new ThrottlingException("Got throttled while preparing checkpoint.", e);
        } catch (InvalidStateException e) {
            String message = "Unable to prepare checkpoint for shardId " + leaseKey;
            log.error(message, e);
            throw new software.amazon.kinesis.exceptions.InvalidStateException(message, e);
        } catch (DependencyException e) {
            throw new KinesisClientLibDependencyException("Unable to prepare checkpoint for shardId " + leaseKey, e);
        }
    }

    @VisibleForTesting
    public boolean setCheckpoint(String leaseKey, ExtendedSequenceNumber checkpoint, UUID concurrencyToken)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Lease lease = leaseCoordinator.getCurrentlyHeldLease(leaseKey);
        if (lease == null) {
            log.info("Worker {} could not update checkpoint for shard {} because it does not hold the lease",
                    leaseCoordinator.workerIdentifier(), leaseKey);
            return false;
        }

        lease.checkpoint(checkpoint);
        lease.pendingCheckpoint(null);
        lease.pendingCheckpointState(null);
        lease.ownerSwitchesSinceCheckpoint(0L);

        return leaseCoordinator.updateLease(lease, concurrencyToken, operation, leaseKey);
    }

    boolean prepareCheckpoint(String leaseKey, ExtendedSequenceNumber pendingCheckpoint, UUID concurrencyToken, byte[] pendingCheckpointState)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Lease lease = leaseCoordinator.getCurrentlyHeldLease(leaseKey);
        if (lease == null) {
            log.info("Worker {} could not prepare checkpoint for shard {} because it does not hold the lease",
                    leaseCoordinator.workerIdentifier(), leaseKey);
            return false;
        }

        lease.pendingCheckpoint(Objects.requireNonNull(pendingCheckpoint, "pendingCheckpoint should not be null"));
        lease.pendingCheckpointState(pendingCheckpointState);
        return leaseCoordinator.updateLease(lease, concurrencyToken, operation, leaseKey);
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
