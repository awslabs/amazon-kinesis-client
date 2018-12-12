/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * Helper class to sync leases with shards of the Kinesis stream.
 * It will create new leases/activities when it discovers new Kinesis shards (bootstrap/resharding).
 * It deletes leases for shards that have been trimmed from Kinesis, or if we've completed processing it
 * and begun processing it's child shards.
 */
class ShardSyncer {

    private static final Log LOG = LogFactory.getLog(ShardSyncer.class);

    /**
     * Note constructor is private: We use static synchronized methods - this is a utility class.
     */
    private ShardSyncer() {
    }

    static synchronized void bootstrapShardLeases(IKinesisProxy kinesisProxy,
            ILeaseManager<KinesisClientLease> leaseManager,
            InitialPositionInStreamExtended initialPositionInStream,
            boolean cleanupLeasesOfCompletedShards)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {
        syncShardLeases(kinesisProxy, leaseManager, initialPositionInStream, cleanupLeasesOfCompletedShards);
    }

    /**
     * Check and create leases for any new shards (e.g. following a reshard operation).
     * 
     * @param kinesisProxy
     * @param leaseManager
     * @param initialPositionInStream
     * @param expectedClosedShardId If this is not null, we will assert that the shard list we get from Kinesis
     *        shows this shard to be closed (e.g. parent shard must be closed after a reshard operation).
     *        If it is open, we assume this is an race condition around a reshard event and throw
     *        a KinesisClientLibIOException so client can backoff and retry later.
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    static synchronized void checkAndCreateLeasesForNewShards(IKinesisProxy kinesisProxy,
            ILeaseManager<KinesisClientLease> leaseManager,
            InitialPositionInStreamExtended initialPositionInStream,
            boolean cleanupLeasesOfCompletedShards)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {
        syncShardLeases(kinesisProxy, leaseManager, initialPositionInStream, cleanupLeasesOfCompletedShards);
    }

    /**
     * Sync leases with Kinesis shards (e.g. at startup, or when we reach end of a shard).
     * 
     * @param kinesisProxy
     * @param leaseManager
     * @param expectedClosedShardId If this is not null, we will assert that the shard list we get from Kinesis
     *        does not show this shard to be open (e.g. parent shard must be closed after a reshard operation).
     *        If it is still open, we assume this is a race condition around a reshard event and
     *        throw a KinesisClientLibIOException so client can backoff and retry later. If the shard doesn't exist in
     *        Kinesis at all, we assume this is an old/expired shard and continue with the sync operation.
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    private static synchronized void syncShardLeases(IKinesisProxy kinesisProxy,
            ILeaseManager<KinesisClientLease> leaseManager,
            InitialPositionInStreamExtended initialPosition,
            boolean cleanupLeasesOfCompletedShards)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {
        List<Shard> shards = getShardList(kinesisProxy);
        LOG.debug("Num shards: " + shards.size());

        Map<String, Shard> shardIdToShardMap = constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap = constructShardIdToChildShardIdsMap(shardIdToShardMap);
        assertAllParentShardsAreClosed(shardIdToChildShardIdsMap, shardIdToShardMap);
        
        List<KinesisClientLease> currentLeases = leaseManager.listLeases();
        
        List<KinesisClientLease> newLeasesToCreate = determineNewLeasesToCreate(shards, currentLeases, initialPosition);
        LOG.debug("Num new leases to create: " + newLeasesToCreate.size());
        for (KinesisClientLease lease : newLeasesToCreate) {
            long startTimeMillis = System.currentTimeMillis();
            boolean success = false;
            try {
                leaseManager.createLeaseIfNotExists(lease);
                success = true;
            } finally {
                MetricsHelper.addSuccessAndLatency("CreateLease", startTimeMillis, success, MetricsLevel.DETAILED);
            }
        }
        
        List<KinesisClientLease> trackedLeases = new ArrayList<>();
        if (currentLeases != null) {
            trackedLeases.addAll(currentLeases);            
        }
        trackedLeases.addAll(newLeasesToCreate);
        cleanupGarbageLeases(shards, trackedLeases, kinesisProxy, leaseManager);
        if (cleanupLeasesOfCompletedShards) {
            cleanupLeasesOfFinishedShards(currentLeases,
                    shardIdToShardMap,
                    shardIdToChildShardIdsMap,
                    trackedLeases,
                    leaseManager);
        }
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    /** Helper method to detect a race condition between fetching the shards via paginated DescribeStream calls
     * and a reshard operation.
     * @param shardIdToChildShardIdsMap
     * @param shardIdToShardMap
     * @throws KinesisClientLibIOException
     */
    private static void assertAllParentShardsAreClosed(Map<String, Set<String>> shardIdToChildShardIdsMap,
            Map<String, Shard> shardIdToShardMap) throws KinesisClientLibIOException {
        for (String parentShardId : shardIdToChildShardIdsMap.keySet()) {
            Shard parentShard = shardIdToShardMap.get(parentShardId);
            if ((parentShardId == null) || (parentShard.getSequenceNumberRange().getEndingSequenceNumber() == null)) {
                throw new KinesisClientLibIOException("Parent shardId " + parentShardId + " is not closed. " 
                        + "This can happen due to a race condition between describeStream and a reshard operation.");
            }
        }
    }

    /**
     * Helper method to create a shardId->KinesisClientLease map.
     * Note: This has package level access for testing purposes only.
     * @param trackedLeaseList
     * @return
     */
    static Map<String, KinesisClientLease> constructShardIdToKCLLeaseMap(List<KinesisClientLease> trackedLeaseList) {
        Map<String, KinesisClientLease> trackedLeasesMap = new HashMap<>();
        for (KinesisClientLease lease : trackedLeaseList) {
            trackedLeasesMap.put(lease.getLeaseKey(), lease);
        }
        return trackedLeasesMap;
    }

    /**
     * Note: this has package level access for testing purposes. 
     * Useful for asserting that we don't have an incomplete shard list following a reshard operation.
     * We verify that if the shard is present in the shard list, it is closed and its hash key range
     *     is covered by its child shards.
     * @param shards List of all Kinesis shards
     * @param shardIdsOfClosedShards Id of the shard which is expected to be closed
     * @return ShardIds of child shards (children of the expectedClosedShard)
     * @throws KinesisClientLibIOException
     */
    static synchronized void assertClosedShardsAreCoveredOrAbsent(Map<String, Shard> shardIdToShardMap,
            Map<String, Set<String>> shardIdToChildShardIdsMap,
            Set<String> shardIdsOfClosedShards) throws KinesisClientLibIOException {        
        String exceptionMessageSuffix = "This can happen if we constructed the list of shards "
                        + " while a reshard operation was in progress.";
        
        for (String shardId : shardIdsOfClosedShards) {
            Shard shard = shardIdToShardMap.get(shardId);
            if (shard == null) {
                LOG.info("Shard " + shardId + " is not present in Kinesis anymore.");
                continue;
            }
            
            String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
            if (endingSequenceNumber == null) {
                throw new KinesisClientLibIOException("Shard " + shardIdsOfClosedShards
                        + " is not closed. " + exceptionMessageSuffix);
            }

            Set<String> childShardIds = shardIdToChildShardIdsMap.get(shardId);
            if (childShardIds == null) {
                throw new KinesisClientLibIOException("Incomplete shard list: Closed shard " + shardId
                        + " has no children." + exceptionMessageSuffix);
            }

            assertHashRangeOfClosedShardIsCovered(shard, shardIdToShardMap, childShardIds);
        }
    }

    private static synchronized void assertHashRangeOfClosedShardIsCovered(Shard closedShard,
            Map<String, Shard> shardIdToShardMap,
            Set<String> childShardIds) throws KinesisClientLibIOException {
        
        BigInteger startingHashKeyOfClosedShard = new BigInteger(closedShard.getHashKeyRange().getStartingHashKey());
        BigInteger endingHashKeyOfClosedShard = new BigInteger(closedShard.getHashKeyRange().getEndingHashKey());
        BigInteger minStartingHashKeyOfChildren = null;
        BigInteger maxEndingHashKeyOfChildren = null;

        for (String childShardId : childShardIds) {
            Shard childShard = shardIdToShardMap.get(childShardId);
            BigInteger startingHashKey = new BigInteger(childShard.getHashKeyRange().getStartingHashKey());
            if ((minStartingHashKeyOfChildren == null)
                    || (startingHashKey.compareTo(minStartingHashKeyOfChildren) < 0)) {
                minStartingHashKeyOfChildren = startingHashKey;
            }
            BigInteger endingHashKey = new BigInteger(childShard.getHashKeyRange().getEndingHashKey());
            if ((maxEndingHashKeyOfChildren == null)
                    || (endingHashKey.compareTo(maxEndingHashKeyOfChildren) > 0)) {
                maxEndingHashKeyOfChildren = endingHashKey;
            }
        }
        
        if ((minStartingHashKeyOfChildren == null) || (maxEndingHashKeyOfChildren == null)
                || (minStartingHashKeyOfChildren.compareTo(startingHashKeyOfClosedShard) > 0)
                || (maxEndingHashKeyOfChildren.compareTo(endingHashKeyOfClosedShard) < 0)) {
            throw new KinesisClientLibIOException("Incomplete shard list: hash key range of shard "
                    + closedShard.getShardId() + " is not covered by its child shards.");
        }
        
    }
    
    /**
     * Helper method to construct shardId->setOfChildShardIds map.
     * Note: This has package access for testing purposes only.
     * @param shardIdToShardMap
     * @return
     */
    static Map<String, Set<String>> constructShardIdToChildShardIdsMap(
            Map<String, Shard> shardIdToShardMap) {
        Map<String, Set<String>> shardIdToChildShardIdsMap = new HashMap<>();
        for (Map.Entry<String, Shard> entry : shardIdToShardMap.entrySet()) {
            String shardId = entry.getKey();
            Shard shard = entry.getValue();
            String parentShardId = shard.getParentShardId();
            if ((parentShardId != null) && (shardIdToShardMap.containsKey(parentShardId))) {
                Set<String> childShardIds = shardIdToChildShardIdsMap.get(parentShardId);
                if (childShardIds == null) {
                    childShardIds = new HashSet<String>();
                    shardIdToChildShardIdsMap.put(parentShardId, childShardIds);
                }
                childShardIds.add(shardId);
            }
            
            String adjacentParentShardId = shard.getAdjacentParentShardId();
            if ((adjacentParentShardId != null) && (shardIdToShardMap.containsKey(adjacentParentShardId))) {
                Set<String> childShardIds = shardIdToChildShardIdsMap.get(adjacentParentShardId);
                if (childShardIds == null) {
                    childShardIds = new HashSet<String>();
                    shardIdToChildShardIdsMap.put(adjacentParentShardId, childShardIds);
                }
                childShardIds.add(shardId);
            }
        }
        return shardIdToChildShardIdsMap;
    }

    private static List<Shard> getShardList(IKinesisProxy kinesisProxy) throws KinesisClientLibIOException {
        List<Shard> shards = kinesisProxy.getShardList();
        if (shards == null) {
            throw new KinesisClientLibIOException(
                    "Stream is not in ACTIVE OR UPDATING state - will retry getting the shard list.");
        }
        return shards;
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     * 
     * For each open (no ending sequence number) shard that doesn't already have a lease,
     * determine if it is a descendent of any shard which is or will be processed (e.g. for which a lease exists):
     * If so, set checkpoint of the shard to TrimHorizon and also create leases for ancestors if needed.
     * If not, set checkpoint of the shard to the initial position specified by the client.
     * To check if we need to create leases for ancestors, we use the following rules:
     *   * If we began (or will begin) processing data for a shard, then we must reach end of that shard before
     *         we begin processing data from any of its descendants.
     *   * A shard does not start processing data until data from all its parents has been processed.
     * Note, if the initial position is LATEST and a shard has two parents and only one is a descendant - we'll create
     * leases corresponding to both the parents - the parent shard which is not a descendant will have  
     * its checkpoint set to Latest.
     * 
     * We assume that if there is an existing lease for a shard, then either:
     *   * we have previously created a lease for its parent (if it was needed), or
     *   * the parent shard has expired.
     * 
     * For example:
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4 5- shards till epoch 102
     * \ / \ / | |
     * 6 7 4 5- shards from epoch 103 - 205
     * \ / | /\
     * 8 4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * New leases to create: (2, 6, 7, 8, 9, 10)
     * 
     * The leases returned are sorted by the starting sequence number - following the same order
     * when persisting the leases in DynamoDB will ensure that we recover gracefully if we fail
     * before creating all the leases.
     * 
     * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
     * @param currentLeases List of current leases
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
     */
    static List<KinesisClientLease> determineNewLeasesToCreate(List<Shard> shards,
            List<KinesisClientLease> currentLeases,
            InitialPositionInStreamExtended initialPosition) {
        Map<String, KinesisClientLease> shardIdToNewLeaseMap = new HashMap<String, KinesisClientLease>();
        Map<String, Shard> shardIdToShardMapOfAllKinesisShards = constructShardIdToShardMap(shards);

        Set<String> shardIdsOfCurrentLeases = new HashSet<String>();
        for (KinesisClientLease lease : currentLeases) {
            shardIdsOfCurrentLeases.add(lease.getLeaseKey());
            LOG.debug("Existing lease: " + lease);
        }

        List<Shard> openShards = getOpenShards(shards);
        Map<String, Boolean> memoizationContext = new HashMap<>();

        // Iterate over the open shards and find those that don't have any lease entries.
        for (Shard shard : openShards) {
            String shardId = shard.getShardId();
            LOG.debug("Evaluating leases for open shard " + shardId + " and its ancestors.");
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                LOG.debug("Lease for shardId " + shardId + " already exists. Not creating a lease");
            } else {
                LOG.debug("Need to create a lease for shardId " + shardId);
                KinesisClientLease newLease = newKCLLease(shard);
                boolean isDescendant =
                        checkIfDescendantAndAddNewLeasesForAncestors(shardId,
                                initialPosition,
                                shardIdsOfCurrentLeases,
                                shardIdToShardMapOfAllKinesisShards,
                                shardIdToNewLeaseMap,
                                memoizationContext);

                /**
                 * If the shard is a descendant and the specified initial position is AT_TIMESTAMP, then the
                 * checkpoint should be set to AT_TIMESTAMP, else to TRIM_HORIZON. For AT_TIMESTAMP, we will add a
                 * lease just like we do for TRIM_HORIZON. However we will only return back records with server-side
                 * timestamp at or after the specified initial position timestamp.
                 *
                 * Shard structure (each level depicts a stream segment):
                 * 0 1 2 3 4   5   - shards till epoch 102
                 * \ / \ / |   |
                 *  6   7  4   5   - shards from epoch 103 - 205
                 *   \ /   |  /\
                 *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
                 *
                 * Current leases: empty set
                 *
                 * For the above example, suppose the initial position in stream is set to AT_TIMESTAMP with
                 * timestamp value 206. We will then create new leases for all the shards (with checkpoint set to
                 * AT_TIMESTAMP), including the ancestor shards with epoch less than 206. However as we begin
                 * processing the ancestor shards, their checkpoints would be updated to SHARD_END and their leases
                 * would then be deleted since they won't have records with server-side timestamp at/after 206. And
                 * after that we will begin processing the descendant shards with epoch at/after 206 and we will
                 * return the records that meet the timestamp requirement for these shards.
                 */
                if (isDescendant && !initialPosition.getInitialPositionInStream()
                        .equals(InitialPositionInStream.AT_TIMESTAMP)) {
                    newLease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                } else {
                    newLease.setCheckpoint(convertToCheckpoint(initialPosition));
                }
                LOG.debug("Set checkpoint of " + newLease.getLeaseKey() + " to " + newLease.getCheckpoint());
                shardIdToNewLeaseMap.put(shardId, newLease);
            }
        }

        List<KinesisClientLease> newLeasesToCreate = new ArrayList<KinesisClientLease>();
        newLeasesToCreate.addAll(shardIdToNewLeaseMap.values());
        Comparator<? super KinesisClientLease> startingSequenceNumberComparator =
                new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMapOfAllKinesisShards);
        Collections.sort(newLeasesToCreate, startingSequenceNumberComparator);
        return newLeasesToCreate;
    }

    /**
     * Note: Package level access for testing purposes only.
     * Check if this shard is a descendant of a shard that is (or will be) processed.
     * Create leases for the ancestors of this shard as required.
     * See javadoc of determineNewLeasesToCreate() for rules and example.
     * 
     * @param shardId The shardId to check.
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param shardIdsOfCurrentLeases The shardIds for the current leases.
     * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
     * @param shardIdToLeaseMapOfNewShards Add lease POJOs corresponding to ancestors to this map.
     * @param memoizationContext Memoization of shards that have been evaluated as part of the evaluation
     * @return true if the shard is a descendant of any current shard (lease already exists)
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    static boolean checkIfDescendantAndAddNewLeasesForAncestors(String shardId,
            InitialPositionInStreamExtended initialPosition,
            Set<String> shardIdsOfCurrentLeases,
            Map<String, Shard> shardIdToShardMapOfAllKinesisShards,
            Map<String, KinesisClientLease> shardIdToLeaseMapOfNewShards,
            Map<String, Boolean> memoizationContext) {
        
        Boolean previousValue = memoizationContext.get(shardId);
        if (previousValue != null) {
            return previousValue;
        }

        boolean isDescendant = false;
        Shard shard;
        Set<String> parentShardIds;
        Set<String> descendantParentShardIds = new HashSet<String>();

        if ((shardId != null) && (shardIdToShardMapOfAllKinesisShards.containsKey(shardId))) {
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                // This shard is a descendant of a current shard.
                isDescendant = true;
                // We don't need to add leases of its ancestors,
                // because we'd have done it when creating a lease for this shard.
            } else {
                shard = shardIdToShardMapOfAllKinesisShards.get(shardId);
                parentShardIds = getParentShardIds(shard, shardIdToShardMapOfAllKinesisShards);
                for (String parentShardId : parentShardIds) {
                    // Check if the parent is a descendant, and include its ancestors.
                    if (checkIfDescendantAndAddNewLeasesForAncestors(parentShardId,
                            initialPosition,
                            shardIdsOfCurrentLeases,
                            shardIdToShardMapOfAllKinesisShards,
                            shardIdToLeaseMapOfNewShards,
                            memoizationContext)) {
                        isDescendant = true;
                        descendantParentShardIds.add(parentShardId);
                        LOG.debug("Parent shard " + parentShardId + " is a descendant.");
                    } else {
                        LOG.debug("Parent shard " + parentShardId + " is NOT a descendant.");
                    }
                }

                // If this is a descendant, create leases for its parent shards (if they don't exist)
                if (isDescendant) {
                    for (String parentShardId : parentShardIds) {
                        if (!shardIdsOfCurrentLeases.contains(parentShardId)) {
                            LOG.debug("Need to create a lease for shardId " + parentShardId);
                            KinesisClientLease lease = shardIdToLeaseMapOfNewShards.get(parentShardId);
                            if (lease == null) {
                                lease = newKCLLease(shardIdToShardMapOfAllKinesisShards.get(parentShardId));
                                shardIdToLeaseMapOfNewShards.put(parentShardId, lease);
                            }

                            if (descendantParentShardIds.contains(parentShardId)
                                    && !initialPosition.getInitialPositionInStream()
                                        .equals(InitialPositionInStream.AT_TIMESTAMP)) {
                                lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                            } else {
                                lease.setCheckpoint(convertToCheckpoint(initialPosition));
                            }
                        }
                    }
                } else {
                    // This shard should be included, if the customer wants to process all records in the stream or
                    // if the initial position is AT_TIMESTAMP. For AT_TIMESTAMP, we will add a lease just like we do
                    // for TRIM_HORIZON. However we will only return back records with server-side timestamp at or
                    // after the specified initial position timestamp.
                    if (initialPosition.getInitialPositionInStream().equals(InitialPositionInStream.TRIM_HORIZON)
                            || initialPosition.getInitialPositionInStream()
                                .equals(InitialPositionInStream.AT_TIMESTAMP)) {
                        isDescendant = true;
                    }
                }

            }
        }

        memoizationContext.put(shardId, isDescendant);
        return isDescendant;
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    /**
     * Helper method to get parent shardIds of the current shard - includes the parent shardIds if:
     * a/ they are not null
     * b/ if they exist in the current shard map (i.e. haven't expired)
     * 
     * @param shard Will return parents of this shard
     * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
     * @return Set of parentShardIds
     */
    static Set<String> getParentShardIds(Shard shard, Map<String, Shard> shardIdToShardMapOfAllKinesisShards) {
        Set<String> parentShardIds = new HashSet<String>(2);
        String parentShardId = shard.getParentShardId();
        if ((parentShardId != null) && shardIdToShardMapOfAllKinesisShards.containsKey(parentShardId)) {
            parentShardIds.add(parentShardId);
        }
        String adjacentParentShardId = shard.getAdjacentParentShardId();
        if ((adjacentParentShardId != null) && shardIdToShardMapOfAllKinesisShards.containsKey(adjacentParentShardId)) {
            parentShardIds.add(adjacentParentShardId);
        }
        return parentShardIds;
    }

    /**
     * Delete leases corresponding to shards that no longer exist in the stream. 
     * Current scheme: Delete a lease if:
     *   * the corresponding shard is not present in the list of Kinesis shards, AND
     *   * the parentShardIds listed in the lease are also not present in the list of Kinesis shards.
     * @param shards List of all Kinesis shards (assumed to be a consistent snapshot - when stream is in Active state).
     * @param trackedLeases List of 
     * @param kinesisProxy Kinesis proxy (used to get shard list)
     * @param leaseManager 
     * @throws KinesisClientLibIOException Thrown if we couldn't get a fresh shard list from Kinesis.
     * @throws ProvisionedThroughputException 
     * @throws InvalidStateException 
     * @throws DependencyException 
     */
    private static void cleanupGarbageLeases(List<Shard> shards,
            List<KinesisClientLease> trackedLeases,
            IKinesisProxy kinesisProxy,
            ILeaseManager<KinesisClientLease> leaseManager)
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException {
        Set<String> kinesisShards = new HashSet<>();
        for (Shard shard : shards) {
            kinesisShards.add(shard.getShardId());
        }
        
        // Check if there are leases for non-existent shards
        List<KinesisClientLease> garbageLeases = new ArrayList<>();
        for (KinesisClientLease lease : trackedLeases) {
            if (isCandidateForCleanup(lease, kinesisShards)) {
                garbageLeases.add(lease);
            }
        }
        
        if (!garbageLeases.isEmpty()) {
            LOG.info("Found " + garbageLeases.size()
                    + " candidate leases for cleanup. Refreshing list of" 
                    + " Kinesis shards to pick up recent/latest shards");
            List<Shard> currentShardList = getShardList(kinesisProxy);
            Set<String> currentKinesisShardIds = new HashSet<>();
            for (Shard shard : currentShardList) {
                currentKinesisShardIds.add(shard.getShardId());
            }

            for (KinesisClientLease lease : garbageLeases) {
                if (isCandidateForCleanup(lease, currentKinesisShardIds)) {
                    LOG.info("Deleting lease for shard " + lease.getLeaseKey()
                            + " as it is not present in Kinesis stream.");
                    leaseManager.deleteLease(lease);
                }
            }
        }
        
    }

    /**
     * Note: This method has package level access, solely for testing purposes.
     * 
     * @param lease Candidate shard we are considering for deletion.
     * @param currentKinesisShardIds
     * @return true if neither the shard (corresponding to the lease), nor its parents are present in
     *         currentKinesisShardIds
     * @throws KinesisClientLibIOException Thrown if currentKinesisShardIds contains a parent shard but not the child
     *         shard (we are evaluating for deletion).
     */
    static boolean isCandidateForCleanup(KinesisClientLease lease, Set<String> currentKinesisShardIds)
        throws KinesisClientLibIOException {
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
    
    /**
     * Private helper method.
     * Clean up leases for shards that meet the following criteria:
     * a/ the shard has been fully processed (checkpoint is set to SHARD_END)
     * b/ we've begun processing all the child shards: we have leases for all child shards and their checkpoint is not
     *      TRIM_HORIZON.
     * 
     * @param currentLeases List of leases we evaluate for clean up
     * @param shardIdToShardMap Map of shardId->Shard (assumed to include all Kinesis shards)
     * @param shardIdToChildShardIdsMap Map of shardId->childShardIds (assumed to include all Kinesis shards)
     * @param trackedLeases List of all leases we are tracking.
     * @param leaseManager Lease manager (will be used to delete leases)
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    private static synchronized void cleanupLeasesOfFinishedShards(Collection<KinesisClientLease> currentLeases,
            Map<String, Shard> shardIdToShardMap,
            Map<String, Set<String>> shardIdToChildShardIdsMap,
            List<KinesisClientLease> trackedLeases,
            ILeaseManager<KinesisClientLease> leaseManager)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {
        Set<String> shardIdsOfClosedShards = new HashSet<>();
        List<KinesisClientLease> leasesOfClosedShards = new ArrayList<>();
        for (KinesisClientLease lease : currentLeases) {
            if (lease.getCheckpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
                shardIdsOfClosedShards.add(lease.getLeaseKey());
                leasesOfClosedShards.add(lease);
            }
        }

        if (!leasesOfClosedShards.isEmpty()) {
            assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap,
                    shardIdToChildShardIdsMap,
                    shardIdsOfClosedShards);
            Comparator<? super KinesisClientLease> startingSequenceNumberComparator =
                    new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMap);
            Collections.sort(leasesOfClosedShards, startingSequenceNumberComparator);
            Map<String, KinesisClientLease> trackedLeaseMap = constructShardIdToKCLLeaseMap(trackedLeases);

            for (KinesisClientLease leaseOfClosedShard : leasesOfClosedShards) {
                String closedShardId = leaseOfClosedShard.getLeaseKey();
                Set<String> childShardIds = shardIdToChildShardIdsMap.get(closedShardId);
                if ((closedShardId != null) && (childShardIds != null) && (!childShardIds.isEmpty())) {
                    cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, leaseManager);
                }
            }
        }        
    }

    /** 
     * Delete lease for the closed shard. Rules for deletion are:
     * a/ the checkpoint for the closed shard is SHARD_END,
     * b/ there are leases for all the childShardIds and their checkpoint is NOT TRIM_HORIZON
     * Note: This method has package level access solely for testing purposes.
     * 
     * @param closedShardId Identifies the closed shard
     * @param childShardIds ShardIds of children of the closed shard
     * @param trackedLeases shardId->KinesisClientLease map with all leases we are tracking (should not be null)
     * @param leaseManager 
     * @throws ProvisionedThroughputException 
     * @throws InvalidStateException 
     * @throws DependencyException 
     */
    static synchronized void cleanupLeaseForClosedShard(String closedShardId,
            Set<String> childShardIds,
            Map<String, KinesisClientLease> trackedLeases,
            ILeaseManager<KinesisClientLease> leaseManager)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        KinesisClientLease leaseForClosedShard = trackedLeases.get(closedShardId);
        List<KinesisClientLease> childShardLeases = new ArrayList<>();
        
        for (String childShardId : childShardIds) {
            KinesisClientLease childLease = trackedLeases.get(childShardId);
            if (childLease != null) {
                childShardLeases.add(childLease);
            }
        }
        
        if ((leaseForClosedShard != null)
                && (leaseForClosedShard.getCheckpoint().equals(ExtendedSequenceNumber.SHARD_END))
                && (childShardLeases.size() == childShardIds.size())) {
            boolean okayToDelete = true;
            for (KinesisClientLease lease : childShardLeases) {
                if (lease.getCheckpoint().equals(ExtendedSequenceNumber.TRIM_HORIZON)) {
                    okayToDelete = false;
                    break;
                }
            }
            
            if (okayToDelete) {
                LOG.info("Deleting lease for shard " + leaseForClosedShard.getLeaseKey()
                        + " as it has been completely processed and processing of child shards has begun.");
                leaseManager.deleteLease(leaseForClosedShard);
            }
        }
    }

    /**
     * Helper method to create a new KinesisClientLease POJO for a shard.
     * Note: Package level access only for testing purposes
     * 
     * @param shard
     * @return
     */
    static KinesisClientLease newKCLLease(Shard shard) {
        KinesisClientLease newLease = new KinesisClientLease();
        newLease.setLeaseKey(shard.getShardId());
        List<String> parentShardIds = new ArrayList<String>(2);
        if (shard.getParentShardId() != null) {
            parentShardIds.add(shard.getParentShardId());
        }
        if (shard.getAdjacentParentShardId() != null) {
            parentShardIds.add(shard.getAdjacentParentShardId());
        }
        newLease.setParentShardIds(parentShardIds);
        newLease.setOwnerSwitchesSinceCheckpoint(0L);

        return newLease;
    }

    /**
     * Helper method to construct a shardId->Shard map for the specified list of shards.
     * 
     * @param shards List of shards
     * @return ShardId->Shard map
     */
    static Map<String, Shard> constructShardIdToShardMap(List<Shard> shards) {
        Map<String, Shard> shardIdToShardMap = new HashMap<String, Shard>();
        for (Shard shard : shards) {
            shardIdToShardMap.put(shard.getShardId(), shard);
        }
        return shardIdToShardMap;
    }

    /**
     * Helper method to return all the open shards for a stream.
     * Note: Package level access only for testing purposes.
     * 
     * @param allShards All shards returved via DescribeStream. We assume this to represent a consistent shard list.
     * @return List of open shards (shards at the tip of the stream) - may include shards that are not yet active.
     */
    static List<Shard> getOpenShards(List<Shard> allShards) {
        List<Shard> openShards = new ArrayList<Shard>();
        for (Shard shard : allShards) {
            String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
            if (endingSequenceNumber == null) {
                openShards.add(shard);
                LOG.debug("Found open shard: " + shard.getShardId());
            }
        }
        return openShards;
    }

    private static ExtendedSequenceNumber convertToCheckpoint(InitialPositionInStreamExtended position) {
        ExtendedSequenceNumber checkpoint = null;
        
        if (position.getInitialPositionInStream().equals(InitialPositionInStream.TRIM_HORIZON)) {
            checkpoint = ExtendedSequenceNumber.TRIM_HORIZON;
        } else if (position.getInitialPositionInStream().equals(InitialPositionInStream.LATEST)) {
            checkpoint = ExtendedSequenceNumber.LATEST;
        } else if (position.getInitialPositionInStream().equals(InitialPositionInStream.AT_TIMESTAMP)) {
            checkpoint = ExtendedSequenceNumber.AT_TIMESTAMP;
        }
        
        return checkpoint;
    }
    
    /** Helper class to compare leases based on starting sequence number of the corresponding shards.
     *
     */
    private static class StartingSequenceNumberAndShardIdBasedComparator implements Comparator<KinesisClientLease>,
            Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<String, Shard> shardIdToShardMap;
        
        /**
         * @param shardIdToShardMapOfAllKinesisShards
         */
        public StartingSequenceNumberAndShardIdBasedComparator(Map<String, Shard> shardIdToShardMapOfAllKinesisShards) {
            shardIdToShardMap = shardIdToShardMapOfAllKinesisShards;
        }

        /**
         * Compares two leases based on the starting sequence number of corresponding shards.
         * If shards are not found in the shardId->shard map supplied, we do a string comparison on the shardIds.
         * We assume that lease1 and lease2 are:
         *     a/ not null,
         *     b/ shards (if found) have non-null starting sequence numbers
         * 
         * {@inheritDoc}
         */
        @Override
        public int compare(KinesisClientLease lease1, KinesisClientLease lease2) {
            int result = 0;
            String shardId1 = lease1.getLeaseKey();
            String shardId2 = lease2.getLeaseKey();
            Shard shard1 = shardIdToShardMap.get(shardId1);
            Shard shard2 = shardIdToShardMap.get(shardId2);
            
            // If we found shards for the two leases, use comparison of the starting sequence numbers
            if ((shard1 != null) && (shard2 != null)) {
                BigInteger sequenceNumber1 =
                        new BigInteger(shard1.getSequenceNumberRange().getStartingSequenceNumber());
                BigInteger sequenceNumber2 =
                        new BigInteger(shard2.getSequenceNumberRange().getStartingSequenceNumber());
                result = sequenceNumber1.compareTo(sequenceNumber2);                
            }
            
            if (result == 0) {
                result = shardId1.compareTo(shardId2);
            }
            
            return result;
        }
                
    }

}
