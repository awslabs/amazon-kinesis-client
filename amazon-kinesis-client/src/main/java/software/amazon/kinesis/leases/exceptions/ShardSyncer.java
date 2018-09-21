package software.amazon.kinesis.leases.exceptions;

import lombok.NonNull;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.HierarchichalShardSyncer;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.metrics.MetricsScope;

/**
 * This class is deprecated
 * Helper class to sync leases with shards of the Kinesis stream.
 */
@Deprecated
public class ShardSyncer {
    private static final HierarchichalShardSyncer HIERARCHICHAL_SHARD_SYNCER = new HierarchichalShardSyncer();

    /**
     * This class is deprecated
     * Class level synchronization
     *
     * @param shardDetector
     * @param leaseRefresher
     * @param initialPosition
     * @param cleanupLeasesOfCompletedShards
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
            final boolean cleanupLeasesOfCompletedShards, final boolean ignoreUnexpectedChildShards,
            final MetricsScope scope) throws DependencyException, InvalidStateException,
            ProvisionedThroughputException, KinesisClientLibIOException {
        HIERARCHICHAL_SHARD_SYNCER.checkAndCreateLeaseForNewShards(
                shardDetector, leaseRefresher, initialPosition, cleanupLeasesOfCompletedShards, ignoreUnexpectedChildShards, scope);
    }
}
