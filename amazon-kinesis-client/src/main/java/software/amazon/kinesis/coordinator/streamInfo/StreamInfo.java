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

    private final String streamId;

    public StreamInfo(final String key, final String streamId, final String entityType) {
        setKey(key);
        this.streamId = streamId;
        setEntityType(entityType);
    }

    public HashMap<String, AttributeValue> serialize() {
        final HashMap<String, AttributeValue> result = new HashMap<>();
        result.put(COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME, AttributeValue.fromS(String.valueOf(getKey())));
        result.put(STREAM_ID_ATTRIBUTE_NAME, AttributeValue.fromS(String.valueOf(streamId)));
        result.put(ENTITY_TYPE_ATTRIBUTE_NAME, AttributeValue.fromS(String.valueOf(entityType)));
        return result;
    }

    public static StreamInfo deserialize(final String key, final Map<String, AttributeValue> attributes) {
        final String streamId = attributes.get(STREAM_ID_ATTRIBUTE_NAME).s();
        final String entityType = attributes.get(ENTITY_TYPE_ATTRIBUTE_NAME).s();
        return new StreamInfo(key, streamId, entityType);
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
