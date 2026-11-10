/*
 * Copyright 2026 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.segmenting;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * Data access object for the leader version items ({@link LeaderVersionState}). Keeps the version of each leader in
 * its own coordinator-state item instead of on the leader lock, and owns the heartbeat that keeps this worker's item
 * fresh while it holds a leader lock.
 *
 * <p>Only the holder of a leader lock writes that lock's version item:</p>
 * <ul>
 *   <li>{@link #setHeldLockKey} is called by the leader decider with the lock this worker holds, or null. When the
 *       held key changes to a lock, the item is written in full (owner, version, heartbeat) on the heartbeat
 *       thread.</li>
 *   <li>Each heartbeat first checks the lock client's local state that this worker still holds an unexpired lock
 *       for that key. For a held lock this needs no DynamoDB call. If the lock isn't held, nothing is written.</li>
 *   <li>The heartbeat write is conditional on the item's {@code ownerName} and {@code versionHash} being this
 *       worker's. A new leader overwrites {@code ownerName} when it acquires the lock, so a previous leader's
 *       heartbeat fails from then on and it stops heartbeating that key.</li>
 *   <li>If the item is missing (e.g. the write after acquiring failed), the heartbeat writes it again.</li>
 * </ul>
 * <p>When the lock is released the item isn't deleted. Its heartbeat stops, so it expires.</p>
 */
@Slf4j
@KinesisClientInternalApi
public class LeaderVersionDAO {

    private final CoordinatorStateDAO coordinatorStateDAO;
    private final String versionHash;
    private final String workerId;
    private final ScheduledExecutorService heartbeatExecutor;
    private final BooleanSupplier enabled;
    private final LongSupplier clock;

    /** Lock key this worker holds ("Leader" or "DeployingLeader"), or null. Set by the leader decider. */
    private volatile String heldLockKey = null;

    /** True until the held lock's version item has been written in full since the lock was acquired. */
    private volatile boolean fullWritePending = false;

    /**
     * Lock key whose version item another worker owns. Heartbeats for it stop until the held key changes, so this
     * worker never fights the item's new owner.
     */
    private volatile String supersededLockKey = null;

    /**
     * @param coordinatorStateDAO DAO the version items are read from and written to
     * @param versionHash this worker's version hash
     * @param workerId this worker's identifier, written as the item owner
     * @param heartbeatExecutor runs the heartbeat. Owned by this DAO and stopped by {@link #shutdown()}
     * @param heartbeatPeriodMillis heartbeat period
     * @param scheduleHeartbeat whether to schedule the heartbeat (false when segmenting is disabled by config)
     * @param enabled whether segmenting is active right now; checked on every heartbeat
     */
    public LeaderVersionDAO(
            final CoordinatorStateDAO coordinatorStateDAO,
            final String versionHash,
            final String workerId,
            final ScheduledExecutorService heartbeatExecutor,
            final long heartbeatPeriodMillis,
            final boolean scheduleHeartbeat,
            final BooleanSupplier enabled) {
        this(
                coordinatorStateDAO,
                versionHash,
                workerId,
                heartbeatExecutor,
                heartbeatPeriodMillis,
                scheduleHeartbeat,
                enabled,
                System::currentTimeMillis);
    }

    @VisibleForTesting
    LeaderVersionDAO(
            final CoordinatorStateDAO coordinatorStateDAO,
            final String versionHash,
            final String workerId,
            final ScheduledExecutorService heartbeatExecutor,
            final long heartbeatPeriodMillis,
            final boolean scheduleHeartbeat,
            final BooleanSupplier enabled,
            final LongSupplier clock) {
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.versionHash = versionHash;
        this.workerId = workerId;
        this.heartbeatExecutor = heartbeatExecutor;
        this.enabled = enabled;
        this.clock = clock;
        if (scheduleHeartbeat) {
            heartbeatExecutor.scheduleAtFixedRate(this::heartbeat, 0, heartbeatPeriodMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Reads the version item for a leader lock.
     *
     * @param lockKey "Leader" or "DeployingLeader"
     * @return the item, or null if it doesn't exist
     */
    public LeaderVersionState getVersionState(final String lockKey)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final String versionKey = LeaderVersionState.versionKeyForLock(lockKey);
        final CoordinatorState state = coordinatorStateDAO.getCoordinatorState(versionKey);
        if (state == null) {
            return null;
        }
        if (state instanceof LeaderVersionState) {
            return (LeaderVersionState) state;
        }
        // e.g. an item written without the entityType attribute, which deserializes as a generic state.
        return LeaderVersionState.deserialize(
                versionKey, state.getAttributes() == null ? new HashMap<>() : state.getAttributes());
    }

    /**
     * Records which leader lock this worker holds. Called by the leader decider after every leadership check. When
     * the key changes to a lock, the lock's version item is written in full right away on the heartbeat thread.
     *
     * @param lockKey the held lock key, or null if this worker holds neither lock
     */
    public void setHeldLockKey(final String lockKey) {
        final String previous = heldLockKey;
        heldLockKey = lockKey;
        if (lockKey == null || lockKey.equals(previous)) {
            return;
        }
        fullWritePending = true;
        supersededLockKey = null;
        try {
            heartbeatExecutor.execute(this::heartbeat);
        } catch (final RejectedExecutionException e) {
            // Shutting down; the version item expires on its own.
            log.debug("Heartbeat executor rejected the version write for {}.", lockKey);
        }
    }

    /** Stops the heartbeat. Called during Scheduler shutdown, before the leader lock is released. */
    public void shutdown() {
        heldLockKey = null;
        if (heartbeatExecutor != null && !heartbeatExecutor.isShutdown()) {
            log.info("Shutting down leader version heartbeat for worker {}.", workerId);
            // shutdownNow so a queued heartbeat doesn't run after the lock is released.
            heartbeatExecutor.shutdownNow();
        }
    }

    /**
     * One heartbeat tick. Package-private for tests. Catches everything: an exception escaping a scheduleAtFixedRate
     * task cancels all later heartbeats.
     */
    @VisibleForTesting
    synchronized void heartbeat() {
        final String lockKey = heldLockKey;
        try {
            if (lockKey == null || !enabled.getAsBoolean() || lockKey.equals(supersededLockKey)) {
                return;
            }
            if (!holdsLock(lockKey)) {
                log.debug("Not heartbeating {} version: this worker doesn't hold the lock.", lockKey);
                return;
            }
            if (fullWritePending) {
                writeVersionState(lockKey);
                fullWritePending = false;
                return;
            }
            if (updateHeartbeat(lockKey)) {
                return;
            }
            // The conditional update failed: the item is missing, or another worker owns it.
            final LeaderVersionState current = getVersionState(lockKey);
            if (current == null) {
                log.info("Version item for {} is missing; writing it again.", lockKey);
                writeVersionState(lockKey);
            } else if (!workerId.equals(current.getOwnerName())) {
                log.info(
                        "Version item for {} is owned by {}; stopping heartbeat for it.",
                        lockKey,
                        current.getOwnerName());
                supersededLockKey = lockKey;
            } else {
                // Owned by this worker with a different version hash, e.g. written before a restart on another
                // version. This worker holds the lock, so its version is the right one.
                log.info(
                        "Version item for {} has version {}; writing {}.",
                        lockKey,
                        current.getVersionHash(),
                        versionHash);
                writeVersionState(lockKey);
            }
        } catch (final Exception e) {
            log.error("Failed to heartbeat leader version item for {}.", lockKey, e);
        }
    }

    /**
     * Whether the lock client holds an unexpired lock for {@code lockKey} owned by this worker. A released lock is
     * removed from the lock client's local map, so it reads as not held.
     */
    private boolean holdsLock(final String lockKey) {
        final AmazonDynamoDBLockClient lockClient = coordinatorStateDAO.getDDBLockClient();
        if (lockClient == null) {
            return false;
        }
        // Held locks are in the lock client's local map, so this does no I/O while the lock is held.
        final Optional<LockItem> lock = lockClient.getLock(lockKey, Optional.empty());
        return lock.isPresent()
                && !lock.get().isExpired()
                && workerId.equals(lock.get().getOwnerName());
    }

    /** Writes the whole item: update if it exists, otherwise create it. */
    private void writeVersionState(final String lockKey)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final LeaderVersionState state = newState(lockKey);
        if (coordinatorStateDAO.updateCoordinatorStateWithExpectation(state, null)) {
            return;
        }
        if (coordinatorStateDAO.createCoordinatorStateIfNotExists(state)) {
            return;
        }
        // Created between the two calls; it exists now, so update it.
        if (!coordinatorStateDAO.updateCoordinatorStateWithExpectation(state, null)) {
            throw new DependencyException(
                    "Unable to write leader version item for " + lockKey, new IllegalStateException());
        }
    }

    /** Refreshes the heartbeat only if the item's owner and version are this worker's. */
    private boolean updateHeartbeat(final String lockKey)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final Map<String, ExpectedAttributeValue> expectations = new HashMap<>();
        expectations.put(
                LeaderVersionState.OWNER_NAME_ATTRIBUTE_NAME,
                ExpectedAttributeValue.builder()
                        .value(AttributeValue.fromS(workerId))
                        .build());
        expectations.put(
                LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME,
                ExpectedAttributeValue.builder()
                        .value(AttributeValue.fromS(versionHash))
                        .build());
        return coordinatorStateDAO.updateCoordinatorStateWithExpectation(newState(lockKey), expectations);
    }

    private LeaderVersionState newState(final String lockKey) {
        return new LeaderVersionState(
                LeaderVersionState.versionKeyForLock(lockKey), versionHash, workerId, clock.getAsLong());
    }
}
