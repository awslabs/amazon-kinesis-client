package software.amazon.kinesis.leases;

import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

public interface LeaseSynchronizer {

    BiFunction<Lease, HierarchicalShardSyncer.MultiStreamArgs, String> shardIdFromLeaseDeducer =
            (lease, multiStreamArgs) ->
                    multiStreamArgs.isMultiStreamMode() ?
                            ((MultiStreamLease) lease).shardId() :
                            lease.leaseKey();

    List<Lease> determineNewLeasesToCreate(List<Shard> shards, List<Lease> currentLeases,
                                           InitialPositionInStreamExtended initialPosition, Set<String> inconsistentShardIds,
                                           HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs);

    void cleanupGarbageLeases(List<Shard> shards, LeaseRefresher leaseRefresher, List<Lease> trackedLeases,
                              HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException;

    void cleanupLeasesOfFinishedShards(LeaseRefresher leaseRefresher, List<Lease> currentLeases, List<Lease> trackedLeases,
                                       HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException;
}


