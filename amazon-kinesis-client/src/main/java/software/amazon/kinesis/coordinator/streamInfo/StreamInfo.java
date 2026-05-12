package software.amazon.kinesis.coordinator.streamInfo;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;

/**
 * Data model of the StreamInfo state. This is used to track StreamMetadata
 */
@Getter
@ToString(callSuper = true)
@Slf4j
@KinesisClientInternalApi
public class StreamInfo extends CoordinatorState {
    public static final String STREAM_ID_ATTRIBUTE_NAME = "streamId";
    public static final String ENTITY_TYPE_ATTRIBUTE_NAME = "entityType";
    public static final String ENTITY_TYPE = "STREAM";
    public static final String MODIFIED_TIMESTAMP_ATTRIBUTE_NAME = "mts";

    private final String streamId;
    private long modifiedTimestamp;

    public StreamInfo(final String key, final String streamId) {
        this(key, streamId, System.currentTimeMillis());
    }

    public StreamInfo(final String key, final String streamId, long modifiedTimestamp) {
        setKey(key);
        this.streamId = streamId;
        setEntityType(ENTITY_TYPE);
        this.modifiedTimestamp = modifiedTimestamp;
    }

    public HashMap<String, AttributeValue> serialize() {
        final HashMap<String, AttributeValue> result = new HashMap<>();
        result.put(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME, AttributeValue.fromS(String.valueOf(getKey())));
        result.put(STREAM_ID_ATTRIBUTE_NAME, AttributeValue.fromS(String.valueOf(streamId)));
        result.put(ENTITY_TYPE_ATTRIBUTE_NAME, AttributeValue.fromS(String.valueOf(entityType)));
        result.put(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN(String.valueOf(modifiedTimestamp)));
        return result;
    }

    public static StreamInfo deserialize(final String key, final Map<String, AttributeValue> attributes) {
        final String streamId = attributes.get(STREAM_ID_ATTRIBUTE_NAME).s();

        // parse modified timestamp attribute; may not exist
        AttributeValue mts = attributes.get(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME);
        final Long modifiedTimestamp = mts == null ? null : Long.parseLong(mts.n());

        // use correct constructor depending on whether modified timestamp was available
        return modifiedTimestamp == null
                ? new StreamInfo(key, streamId)
                : new StreamInfo(key, streamId, modifiedTimestamp);
    }

    public static String multiStreamLeaseKeyToStreamIdentifier(String multiStreamLeaseKey) {
        if (multiStreamLeaseKey == null || multiStreamLeaseKey.isEmpty()) {
            throw new IllegalArgumentException();
        }
        String[] parts = multiStreamLeaseKey.split(":", 4);
        if (parts.length < 3) {
            return multiStreamLeaseKey;
        }
        return parts[0] + ":" + parts[1] + ":" + parts[2];
    }
}
