package software.amazon.kinesis.common;

import com.google.common.base.Joiner;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import software.amazon.awssdk.utils.Validate;

import java.util.regex.Pattern;

@RequiredArgsConstructor
@EqualsAndHashCode
@Getter
@Accessors(fluent = true)
public class StreamIdentifier {
    private final String accountName;
    private final String streamName;
    private final Long streamCreationEpoch;

    private static final String DEFAULT_ACCOUNT = "default";
    private static final String DELIMITER = ":";
    private static final Pattern PATTERN = Pattern.compile(".*" + ":" + ".*" + ":" + "[0-9]*");

    @Override
    public String toString(){
        return Joiner.on(DELIMITER).join(accountName, streamName, streamCreationEpoch);
    }

    public static StreamIdentifier fromString(String streamIdentifier) {
        if (PATTERN.matcher(streamIdentifier).matches()) {
            final String[] split = streamIdentifier.split(DELIMITER);
            return new StreamIdentifier(split[0], split[1], Long.parseLong(split[2]));
        } else {
            throw new IllegalArgumentException("Unable to deserialize StreamIdentifier from " + streamIdentifier);
        }
    }

    public static StreamIdentifier fromStreamName(String streamName) {
        Validate.notEmpty(streamName, "StreamName should not be empty");
        return new StreamIdentifier(DEFAULT_ACCOUNT, streamName, 0L);
    }
}
