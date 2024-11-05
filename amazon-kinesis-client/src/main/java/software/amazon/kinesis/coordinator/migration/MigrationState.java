package software.amazon.kinesis.coordinator.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.kinesis.common.StackTraceUtils;
import software.amazon.kinesis.coordinator.CoordinatorState;

/**
 * Data model of the Migration state. This is used to track the state related to migration
 * from KCLv2.x to KCLv3.x.
 */
@Getter
@ToString(callSuper = true)
@Slf4j
public class MigrationState extends CoordinatorState {
    /**
     * Key value for the item in the CoordinatorState table
     */
    public static final String MIGRATION_HASH_KEY = "Migration3.0";
    /**
     * Attribute name in migration state item, whose value is used during
     * the KCL v3.x migration process to know whether the workers need to
     * perform KCL v2.x compatible operations or can perform native KCL v3.x
     * operations.
     */
    public static final String CLIENT_VERSION_ATTRIBUTE_NAME = "cv";

    public static final String MODIFIED_BY_ATTRIBUTE_NAME = "mb";
    public static final String MODIFIED_TIMESTAMP_ATTRIBUTE_NAME = "mts";
    public static final String HISTORY_ATTRIBUTE_NAME = "h";
    private static final int MAX_HISTORY_ENTRIES = 10;

    private ClientVersion clientVersion;
    private String modifiedBy;
    private long modifiedTimestamp;
    private final List<HistoryEntry> history;

    private MigrationState(
            final String key,
            final ClientVersion clientVersion,
            final String modifiedBy,
            final long modifiedTimestamp,
            final List<HistoryEntry> historyEntries,
            final Map<String, AttributeValue> others) {
        setKey(key);
        setAttributes(others);
        this.clientVersion = clientVersion;
        this.modifiedBy = modifiedBy;
        this.modifiedTimestamp = modifiedTimestamp;
        this.history = historyEntries;
    }

    public MigrationState(final String key, final String modifiedBy) {
        this(
                key,
                ClientVersion.CLIENT_VERSION_INIT,
                modifiedBy,
                System.currentTimeMillis(),
                new ArrayList<>(),
                new HashMap<>());
    }

    public HashMap<String, AttributeValue> serialize() {
        final HashMap<String, AttributeValue> result = new HashMap<>();
        result.put(CLIENT_VERSION_ATTRIBUTE_NAME, AttributeValue.fromS(clientVersion.name()));
        result.put(MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS(modifiedBy));
        result.put(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN(String.valueOf(modifiedTimestamp)));

        if (!history.isEmpty()) {
            final List<AttributeValue> historyList = new ArrayList<>();
            for (final HistoryEntry entry : history) {
                historyList.add(AttributeValue.builder().m(entry.serialize()).build());
            }
            result.put(
                    HISTORY_ATTRIBUTE_NAME,
                    AttributeValue.builder().l(historyList).build());
        }

        return result;
    }

    public static MigrationState deserialize(final String key, final HashMap<String, AttributeValue> attributes) {
        if (!MIGRATION_HASH_KEY.equals(key)) {
            return null;
        }

        try {
            final HashMap<String, AttributeValue> mutableAttributes = new HashMap<>(attributes);
            final ClientVersion clientVersion = ClientVersion.valueOf(
                    mutableAttributes.remove(CLIENT_VERSION_ATTRIBUTE_NAME).s());
            final String modifiedBy =
                    mutableAttributes.remove(MODIFIED_BY_ATTRIBUTE_NAME).s();
            final long modifiedTimestamp = Long.parseLong(
                    mutableAttributes.remove(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME).n());

            final List<HistoryEntry> historyList = new ArrayList<>();
            if (attributes.containsKey(HISTORY_ATTRIBUTE_NAME)) {
                mutableAttributes.remove(HISTORY_ATTRIBUTE_NAME).l().stream()
                        .map(historyEntry -> HistoryEntry.deserialize(historyEntry.m()))
                        .forEach(historyList::add);
            }
            final MigrationState migrationState = new MigrationState(
                    MIGRATION_HASH_KEY, clientVersion, modifiedBy, modifiedTimestamp, historyList, mutableAttributes);

            if (!mutableAttributes.isEmpty()) {
                log.info("Unknown attributes {} for state {}", mutableAttributes, migrationState);
            }
            return migrationState;

        } catch (final Exception e) {
            log.warn("Unable to deserialize state with key {} and attributes {}", key, attributes, e);
        }
        return null;
    }

    public Map<String, ExpectedAttributeValue> getDynamoClientVersionExpectation() {
        return new HashMap<String, ExpectedAttributeValue>() {
            {
                put(
                        CLIENT_VERSION_ATTRIBUTE_NAME,
                        ExpectedAttributeValue.builder()
                                .value(AttributeValue.fromS(clientVersion.name()))
                                .build());
            }
        };
    }

    public MigrationState copy() {
        return new MigrationState(
                getKey(),
                getClientVersion(),
                getModifiedBy(),
                getModifiedTimestamp(),
                new ArrayList<>(getHistory()),
                new HashMap<>(getAttributes()));
    }

    public MigrationState update(final ClientVersion clientVersion, final String modifiedBy) {
        log.info(
                "Migration state is being updated to {} current state {} caller {}",
                clientVersion,
                this,
                StackTraceUtils.getPrintableStackTrace(Thread.currentThread().getStackTrace()));
        addHistoryEntry(this.clientVersion, this.modifiedBy, this.modifiedTimestamp);
        this.clientVersion = clientVersion;
        this.modifiedBy = modifiedBy;
        this.modifiedTimestamp = System.currentTimeMillis();
        return this;
    }

    public void addHistoryEntry(
            final ClientVersion lastClientVersion, final String lastModifiedBy, final long lastModifiedTimestamp) {
        history.add(0, new HistoryEntry(lastClientVersion, lastModifiedBy, lastModifiedTimestamp));
        if (history.size() > MAX_HISTORY_ENTRIES) {
            log.info("Limit {} reached, dropping history {}", MAX_HISTORY_ENTRIES, history.remove(history.size() - 1));
        }
    }

    public Map<String, AttributeValueUpdate> getDynamoUpdate() {
        final HashMap<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put(
                CLIENT_VERSION_ATTRIBUTE_NAME,
                AttributeValueUpdate.builder()
                        .value(AttributeValue.fromS(clientVersion.name()))
                        .action(AttributeAction.PUT)
                        .build());
        updates.put(
                MODIFIED_BY_ATTRIBUTE_NAME,
                AttributeValueUpdate.builder()
                        .value(AttributeValue.fromS(modifiedBy))
                        .action(AttributeAction.PUT)
                        .build());
        updates.put(
                MODIFIED_TIMESTAMP_ATTRIBUTE_NAME,
                AttributeValueUpdate.builder()
                        .value(AttributeValue.fromN(String.valueOf(modifiedTimestamp)))
                        .action(AttributeAction.PUT)
                        .build());
        if (!history.isEmpty()) {
            updates.put(
                    HISTORY_ATTRIBUTE_NAME,
                    AttributeValueUpdate.builder()
                            .value(AttributeValue.fromL(
                                    history.stream().map(HistoryEntry::toAv).collect(Collectors.toList())))
                            .action(AttributeAction.PUT)
                            .build());
        }
        return updates;
    }

    @RequiredArgsConstructor
    @ToString
    public static class HistoryEntry {
        private final ClientVersion lastClientVersion;
        private final String lastModifiedBy;
        private final long lastModifiedTimestamp;

        public AttributeValue toAv() {
            return AttributeValue.fromM(serialize());
        }

        public Map<String, AttributeValue> serialize() {
            return new HashMap<String, AttributeValue>() {
                {
                    put(CLIENT_VERSION_ATTRIBUTE_NAME, AttributeValue.fromS(lastClientVersion.name()));
                    put(MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS(lastModifiedBy));
                    put(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN(String.valueOf(lastModifiedTimestamp)));
                }
            };
        }

        public static HistoryEntry deserialize(final Map<String, AttributeValue> map) {
            return new HistoryEntry(
                    ClientVersion.valueOf(map.get(CLIENT_VERSION_ATTRIBUTE_NAME).s()),
                    map.get(MODIFIED_BY_ATTRIBUTE_NAME).s(),
                    Long.parseLong(map.get(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME).n()));
        }
    }
}
