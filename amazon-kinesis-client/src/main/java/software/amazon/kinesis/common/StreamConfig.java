package software.amazon.kinesis.common;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class StreamConfig {
    // TODO: Consider having streamIdentifier as the unique identifier of this class.
    StreamIdentifier streamIdentifier;
    InitialPositionInStreamExtended initialPositionInStreamExtended;
}


