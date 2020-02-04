package software.amazon.kinesis.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.kinesis.model.KinesisResponseMetadata;

import java.time.Instant;
import java.util.Optional;

@Accessors(fluent = true)
@Getter
@AllArgsConstructor
public class RequestDetails {
    private final String requestId;
    private final String timestamp;
}
