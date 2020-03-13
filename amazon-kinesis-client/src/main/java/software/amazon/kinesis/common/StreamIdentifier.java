package software.amazon.kinesis.common;

import com.google.common.base.Joiner;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import software.amazon.awssdk.utils.Validate;

import java.util.Optional;
import java.util.regex.Pattern;

@EqualsAndHashCode @Getter @Accessors(fluent = true)
public class StreamIdentifier {
    private final Optional<String> accountIdOptional;
    private final String streamName;
    private final Optional<Long> streamCreationEpochOptional;

    private static final String DELIMITER = ":";
    private static final Pattern PATTERN = Pattern.compile(".*" + ":" + ".*" + ":" + "[0-9]*");

    private StreamIdentifier(Optional<String> accountIdOptional, String streamName,
            Optional<Long> streamCreationEpochOptional) {
        Validate.isTrue((accountIdOptional.isPresent() && streamCreationEpochOptional.isPresent()) ||
                        (!accountIdOptional.isPresent() && !streamCreationEpochOptional.isPresent()),
                "AccountId and StreamCreationEpoch must either be present together or not");
        this.accountIdOptional = accountIdOptional;
        this.streamName = streamName;
        this.streamCreationEpochOptional = streamCreationEpochOptional;
    }

    /**
     * Serialize the current StreamIdentifier instance.
     * @return
     */
    public String serialize() {
        return accountIdOptional.isPresent() ?
                Joiner.on(DELIMITER).join(accountIdOptional.get(), streamName, streamCreationEpochOptional.get()) :
                streamName;
    }

    @Override
    public String toString() {
        return serialize();
    }

    /**
     * Create a multi stream instance for StreamIdentifier from serialized stream identifier.
     * @param streamIdentifierSer
     * @return StreamIdentifier
     */
    public static StreamIdentifier multiStreamInstance(String streamIdentifierSer) {
        if (PATTERN.matcher(streamIdentifierSer).matches()) {
            final String[] split = streamIdentifierSer.split(DELIMITER);
            return new StreamIdentifier(Optional.of(split[0]), split[1], Optional.of(Long.parseLong(split[2])));
        } else {
            throw new IllegalArgumentException("Unable to deserialize StreamIdentifier from " + streamIdentifierSer);
        }
    }

    /**
     * Create a single stream instance for StreamIdentifier from stream name.
     * @param streamName
     * @return StreamIdentifier
     */
    public static StreamIdentifier singleStreamInstance(String streamName) {
        Validate.notEmpty(streamName, "StreamName should not be empty");
        return new StreamIdentifier(Optional.empty(), streamName, Optional.empty());
    }
}
