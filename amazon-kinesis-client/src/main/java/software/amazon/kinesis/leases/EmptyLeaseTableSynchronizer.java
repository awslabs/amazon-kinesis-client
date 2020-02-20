package software.amazon.kinesis.leases;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static software.amazon.kinesis.leases.HierarchicalShardSyncer.constructShardIdToShardMap;
import static software.amazon.kinesis.leases.HierarchicalShardSyncer.convertToCheckpoint;
import static software.amazon.kinesis.leases.HierarchicalShardSyncer.newKCLLease;

@Slf4j
@AllArgsConstructor
public class EmptyLeaseTableSynchronizer implements LeaseSynchronizer {

    @Override
    public List<Lease> determineNewLeasesToCreate(List<Shard> shards, List<Lease> currentLeases,
                                                  InitialPositionInStreamExtended initialPosition,
                                                  Set<String> inconsistentShardIds,
                                                  HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs) {

        final Map<String, Shard> shardIdToShardMapOfAllKinesisShards = constructShardIdToShardMap(shards);

        currentLeases.stream().peek(lease -> log.debug("Existing lease: {}", lease))
                .map(lease -> shardIdFromLeaseDeducer.apply(lease, multiStreamArgs))
                .collect(Collectors.toSet());

        final List<Lease> newLeasesToCreate = getLeasesToCreateForOpenAndClosedShards(initialPosition, shards);

        final Comparator<Lease> startingSequenceNumberComparator =
                new HierarchicalShardSyncer.StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMapOfAllKinesisShards, multiStreamArgs);
        newLeasesToCreate.sort(startingSequenceNumberComparator);
        return newLeasesToCreate;
    }

    @Override
    public void cleanupGarbageLeases(List<Shard> shards, LeaseRefresher leaseRefresher, List<Lease> trackedLeases, HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs) throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        // Nothing to do here.
    }

    @Override
    public void cleanupLeasesOfFinishedShards(LeaseRefresher leaseRefresher, List<Lease> currentLeases, List<Lease> trackedLeases, HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs) throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        // Nothing to do here.
    }

    /**
     * Helper method to create leases. For an empty lease table, we will be creating leases for all shards
     * regardless of if they are open or closed. Closed shards will be unblocked via child shard information upon
     * reaching SHARD_END.
     */
    private List<Lease> getLeasesToCreateForOpenAndClosedShards(InitialPositionInStreamExtended initialPosition,
                                                                List<Shard> shards)  {
        final Map<String, Lease> shardIdToNewLeaseMap = new HashMap<>();

        for (Shard shard : shards) {
            final String shardId = shard.shardId();
            final Lease lease = newKCLLease(shard);
            lease.checkpoint(convertToCheckpoint(initialPosition));

            log.debug("Need to create a lease for shard with shardId {}", shardId);

            shardIdToNewLeaseMap.put(shardId, lease);
        }

        return new ArrayList(shardIdToNewLeaseMap.values());
    }
}
