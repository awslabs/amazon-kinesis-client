package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;

import java.util.Set;

/**
 * Represents the class that decides if a lease is eligible for cleanup.
 */
@Deprecated
public interface LeaseCleanupValidator {

    /**
     * @param lease Candidate shard we are considering for deletion.
     * @param currentKinesisShardIds
     * @return boolean representing if the lease is eligible for cleanup.
     * @throws KinesisClientLibIOException
     */
    boolean isCandidateForCleanup(KinesisClientLease lease, Set<String> currentKinesisShardIds)
            throws KinesisClientLibIOException;
}
