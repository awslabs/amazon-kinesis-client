package software.amazon.kinesis.leases.exceptions;

import lombok.NonNull;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.metrics.MetricsScope;

/**
 * Helper class to sync leases with shards of the Kinesis stream.
 * It will create new leases/activities when it discovers new Kinesis shards (bootstrap/resharding).
 * It deletes leases for shards that have been trimmed from Kinesis, or if we've completed processing it
 * and begun processing its child shards.
 *
 * <p>NOTE: This class is deprecated and will be removed in a future release.</p>
 */
@Deprecated
public class ShardSyncer {
    private static final HierarchicalShardSyncer HIERARCHICAL_SHARD_SYNCER = new HierarchicalShardSyncer();

    /**
     * <p>NOTE: This method is deprecated and will be removed in a future release.</p>
     *
     * @param shardDetector
     * @param leaseRefresher
     * @param initialPosition
     * @param ignoreUnexpectedChildShards
     * @param scope
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    @Deprecated
    public static synchronized void checkAndCreateLeasesForNewShards(@NonNull final ShardDetector shardDetector,
            final LeaseRefresher leaseRefresher, final InitialPositionInStreamExtended initialPosition,
            final boolean ignoreUnexpectedChildShards, final MetricsScope scope)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException, InterruptedException {
            HIERARCHICAL_SHARD_SYNCER.checkAndCreateLeaseForNewShards(shardDetector, leaseRefresher, initialPosition,
                    scope, ignoreUnexpectedChildShards, leaseRefresher.isLeaseTableEmpty());
    }
}
