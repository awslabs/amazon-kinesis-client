package software.amazon.kinesis.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Getter
@AllArgsConstructor
public class RequestDetails {
    private final String requestId;
    private final String timestamp;
}
