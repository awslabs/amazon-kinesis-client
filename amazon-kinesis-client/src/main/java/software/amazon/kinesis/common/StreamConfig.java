package software.amazon.kinesis.common;

import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.experimental.FieldDefaults;

@Data
@Accessors(fluent = true)
@FieldDefaults(makeFinal=true, level= AccessLevel.PRIVATE)
public class StreamConfig {
    StreamIdentifier streamIdentifier;
    InitialPositionInStreamExtended initialPositionInStreamExtended;
}


