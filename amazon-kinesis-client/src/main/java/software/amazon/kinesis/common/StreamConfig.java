package software.amazon.kinesis.common;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class StreamConfig {
    StreamIdentifier streamIdentifier;
    InitialPositionInStreamExtended initialPositionInStreamExtended;
}


