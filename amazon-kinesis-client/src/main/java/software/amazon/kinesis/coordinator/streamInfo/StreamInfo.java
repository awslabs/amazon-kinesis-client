package software.amazon.kinesis.coordinator.streamInfo;

import java.util.Map;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.EntityType;

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

    public StreamInfo(final String key, final String streamId) {
        super(key, EntityType.CoordinatorStateType.STREAM_INFO, null);
        this.streamId = streamId;
    }

    @Override
    public Map<String, AttributeValue> serialize() {
        // super.serialize() handles entityType, and generic attributes
        final Map<String, AttributeValue> result = super.serialize();
        result.put(STREAM_ID_ATTRIBUTE_NAME, AttributeValue.fromS(streamId));
        return result;
    }

    public static StreamInfo deserialize(final String key, final Map<String, AttributeValue> attributes) {
        if (attributes == null || !attributes.containsKey(ENTITY_TYPE_ATTRIBUTE_NAME)) {
            return null;
        }
        final AttributeValue entityTypeAttr = attributes.get(ENTITY_TYPE_ATTRIBUTE_NAME);
        if (entityTypeAttr == null
                || !EntityType.CoordinatorStateType.STREAM_INFO.getDdbValue().equals(entityTypeAttr.s())) {
            return null;
        }
        try {
            final String streamId = attributes.get(STREAM_ID_ATTRIBUTE_NAME).s();
            return new StreamInfo(key, streamId);
        } catch (final Exception e) {
            log.warn("Unable to deserialize StreamInfo with key {} and attributes {}", key, attributes, e);
            return null;
        }
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
