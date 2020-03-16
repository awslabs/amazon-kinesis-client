package software.amazon.kinesis.common;

import com.google.common.base.Joiner;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
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

    private StreamIdentifier(@NonNull String accountId, @NonNull String streamName, @NonNull Long streamCreationEpoch) {
        this.accountIdOptional = Optional.of(accountId);
        this.streamName = streamName;
        this.streamCreationEpochOptional = Optional.of(streamCreationEpoch);
    }

    private StreamIdentifier(@NonNull String streamName) {
        this.accountIdOptional = Optional.empty();
        this.streamName = streamName;
        this.streamCreationEpochOptional = Optional.empty();
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
            return new StreamIdentifier(split[0], split[1], Long.parseLong(split[2]));
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
        return new StreamIdentifier(streamName);
    }
}
