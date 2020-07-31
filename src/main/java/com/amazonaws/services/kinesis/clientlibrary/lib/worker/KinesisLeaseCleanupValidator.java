package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Set;

/**
 * Represents the class that decides if a lease is eligible for cleanup.
 */
@Deprecated
class KinesisLeaseCleanupValidator implements LeaseCleanupValidator {

    private static final Log LOG = LogFactory.getLog(KinesisLeaseCleanupValidator.class);

    /**
     * @param lease Candidate shard we are considering for deletion.
     * @param currentKinesisShardIds
     * @return true if neither the shard (corresponding to the lease), nor its parents are present in
     *         currentKinesisShardIds
     * @throws KinesisClientLibIOException Thrown if currentKinesisShardIds contains a parent shard but not the child
     *         shard (we are evaluating for deletion).
     */
    @Override
    public boolean isCandidateForCleanup(KinesisClientLease lease, Set<String> currentKinesisShardIds) throws KinesisClientLibIOException {
        boolean isCandidateForCleanup = true;

        if (currentKinesisShardIds.contains(lease.getLeaseKey())) {
            isCandidateForCleanup = false;
        } else {
            LOG.info("Found lease for non-existent shard: " + lease.getLeaseKey() + ". Checking its parent shards");
            Set<String> parentShardIds = lease.getParentShardIds();
            for (String parentShardId : parentShardIds) {

                // Throw an exception if the parent shard exists (but the child does not).
                // This may be a (rare) race condition between fetching the shard list and Kinesis expiring shards.
                if (currentKinesisShardIds.contains(parentShardId)) {
                    String message =
                            "Parent shard " + parentShardId + " exists but not the child shard "
                                    + lease.getLeaseKey();
                    LOG.info(message);
                    throw new KinesisClientLibIOException(message);
                }
            }
        }

        return isCandidateForCleanup;
    }
}
