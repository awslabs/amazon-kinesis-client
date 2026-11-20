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

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leader.LeaderLock;
import software.amazon.kinesis.leases.EntityType;

/**
 * Data model of a leader's version item. There is one item per leader lock: {@link #LEADER_VERSION_HASH_KEY} holds
 * the version of the worker holding the {@link LeaderLock#LEADER_HASH_KEY} lock, and
 * {@link #DEPLOYING_LEADER_VERSION_HASH_KEY} holds the version of the worker holding the
 * {@link LeaderLock#DEPLOYING_LEADER_HASH_KEY} lock. Only the holder of the matching lock writes the item, and it
 * refreshes {@link #HEARTBEAT_ATTRIBUTE_NAME} while it holds the lock. Other workers read the item to decide which
 * lock to contend for.
 *
 * <p>The item uses the existing {@link EntityType#LEADER_LOCK} entity type. A new entity type would be read as a
 * lease by KCL versions that predate it, because the lease table DAO treats unknown entity types as leases.</p>
 */
@Getter
@ToString(callSuper = true)
@Slf4j
@KinesisClientInternalApi
public class LeaderVersionState extends CoordinatorState {

    public static final String LEADER_VERSION_HASH_KEY = "LeaderVersion";
    public static final String DEPLOYING_LEADER_VERSION_HASH_KEY = "DeployingLeaderVersion";

    public static final String VERSION_HASH_ATTRIBUTE_NAME = "versionHash";
    public static final String OWNER_NAME_ATTRIBUTE_NAME = "ownerName";
    /** Epoch millis of the owner's last heartbeat. */
    public static final String HEARTBEAT_ATTRIBUTE_NAME = "heartbeat";

    private final String versionHash;
    private final String ownerName;
    /** Null when the stored heartbeat is missing or unparseable. Such an item is treated as expired. */
    private final Long heartbeatMillis;

    private LeaderVersionState(
            final String key,
            final String versionHash,
            final String ownerName,
            final Long heartbeatMillis,
            final Map<String, AttributeValue> others) {
        super(key, EntityType.CoordinatorStateType.LEADER_LOCK, others);
        this.versionHash = versionHash;
        this.ownerName = ownerName;
        this.heartbeatMillis = heartbeatMillis;
    }

    public LeaderVersionState(
            final String key, final String versionHash, final String ownerName, final long heartbeatMillis) {
        this(key, versionHash, ownerName, heartbeatMillis, new HashMap<>());
    }

    /**
     * Returns the version item key for a leader lock key.
     *
     * @throws IllegalArgumentException if {@code lockKey} isn't a leader lock key
     */
    public static String versionKeyForLock(final String lockKey) {
        if (LeaderLock.LEADER_HASH_KEY.equals(lockKey)) {
            return LEADER_VERSION_HASH_KEY;
        }
        if (LeaderLock.DEPLOYING_LEADER_HASH_KEY.equals(lockKey)) {
            return DEPLOYING_LEADER_VERSION_HASH_KEY;
        }
        throw new IllegalArgumentException("Not a leader lock key: " + lockKey);
    }

    public static boolean isVersionKey(final String key) {
        return LEADER_VERSION_HASH_KEY.equals(key) || DEPLOYING_LEADER_VERSION_HASH_KEY.equals(key);
    }

    /** Whether the heartbeat is missing or older than {@code expiryMillis} at {@code nowMillis}. */
    public boolean isExpired(final long nowMillis, final long expiryMillis) {
        return heartbeatMillis == null || nowMillis - heartbeatMillis > expiryMillis;
    }

    @Override
    public Map<String, AttributeValue> serialize() {
        final Map<String, AttributeValue> result = new HashMap<>(super.serialize());
        putIfNotNull(
                result, VERSION_HASH_ATTRIBUTE_NAME, versionHash == null ? null : AttributeValue.fromS(versionHash));
        putIfNotNull(result, OWNER_NAME_ATTRIBUTE_NAME, ownerName == null ? null : AttributeValue.fromS(ownerName));
        putIfNotNull(
                result,
                HEARTBEAT_ATTRIBUTE_NAME,
                heartbeatMillis == null ? null : AttributeValue.fromN(String.valueOf(heartbeatMillis)));
        return result;
    }

    /**
     * Returns the state for a version item, or null if {@code key} isn't a version key. Missing attributes are
     * returned as null fields rather than failing, so a partially written item reads as expired instead of breaking
     * the reader.
     */
    public static LeaderVersionState deserialize(final String key, final Map<String, AttributeValue> attributes) {
        if (!isVersionKey(key)) {
            return null;
        }
        final Map<String, AttributeValue> others = new HashMap<>(attributes);
        others.remove(ENTITY_TYPE_ATTRIBUTE_NAME);
        final String versionHash = stringOrNull(others.remove(VERSION_HASH_ATTRIBUTE_NAME));
        final String ownerName = stringOrNull(others.remove(OWNER_NAME_ATTRIBUTE_NAME));
        final AttributeValue heartbeat = others.remove(HEARTBEAT_ATTRIBUTE_NAME);
        Long heartbeatMillis = null;
        if (heartbeat != null && heartbeat.n() != null) {
            try {
                heartbeatMillis = Long.parseLong(heartbeat.n());
            } catch (final NumberFormatException e) {
                log.warn("Unparseable {} '{}' on {}; treating as expired.", HEARTBEAT_ATTRIBUTE_NAME, heartbeat, key);
            }
        }
        return new LeaderVersionState(key, versionHash, ownerName, heartbeatMillis, others);
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoUpdate() {
        final Map<String, AttributeValueUpdate> updates = new HashMap<>();
        // Included so an item created by an older writer gets the entity type too.
        updates.put(
                ENTITY_TYPE_ATTRIBUTE_NAME,
                put(AttributeValue.fromS(EntityType.CoordinatorStateType.LEADER_LOCK.getDdbValue())));
        if (versionHash != null) {
            updates.put(VERSION_HASH_ATTRIBUTE_NAME, put(AttributeValue.fromS(versionHash)));
        }
        if (ownerName != null) {
            updates.put(OWNER_NAME_ATTRIBUTE_NAME, put(AttributeValue.fromS(ownerName)));
        }
        if (heartbeatMillis != null) {
            updates.put(HEARTBEAT_ATTRIBUTE_NAME, put(AttributeValue.fromN(String.valueOf(heartbeatMillis))));
        }
        return updates;
    }

    private static AttributeValueUpdate put(final AttributeValue value) {
        return AttributeValueUpdate.builder()
                .value(value)
                .action(AttributeAction.PUT)
                .build();
    }

    private static void putIfNotNull(
            final Map<String, AttributeValue> map, final String name, final AttributeValue value) {
        if (value != null) {
            map.put(name, value);
        }
    }

    private static String stringOrNull(final AttributeValue value) {
        return value == null ? null : value.s();
    }
}
