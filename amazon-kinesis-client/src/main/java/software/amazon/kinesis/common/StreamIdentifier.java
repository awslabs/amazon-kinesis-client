package software.amazon.kinesis.common;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.utils.Validate;

@RequiredArgsConstructor
@EqualsAndHashCode
@Getter
public class StreamIdentifier {
    private final String accountName;
    private final String streamName;
    private final String streamCreationEpoch;

    private static final String DEFAULT = "default";

    @Override
    public String toString(){
        return accountName + ":" + streamName + ":" + streamCreationEpoch;
    }

    public static StreamIdentifier fromString(String streamIdentifier) {
        final String[] idTokens = streamIdentifier.split(":");
        Validate.isTrue(idTokens.length == 3, "Unable to deserialize StreamIdentifier from " + streamIdentifier);
        return new StreamIdentifier(idTokens[0], idTokens[1], idTokens[2]);
    }

    public static StreamIdentifier fromStreamName(String streamName) {
        Validate.notEmpty(streamName, "StreamName should not be empty");
        return new StreamIdentifier(DEFAULT, streamName, DEFAULT);
    }
}
