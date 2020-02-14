package software.amazon.kinesis.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Optional;

@Accessors(fluent=true)
@Getter
public class RequestDetails {

    /**
     * Placeholder for logging when no successful request has been made.
     */
    private static final String NONE = "NONE";

    private final Optional<String> requestId;
    private final Optional<String> timestamp;

    public RequestDetails() {
        this(Optional.empty(), Optional.empty());
    }

    public RequestDetails(Optional<String> requestId, Optional<String> timestamp) {
        this.requestId = requestId;
        this.timestamp = timestamp;
    }

    /**
     * Gets last successful request's request id.
     *
     * @return requestId associated with last successful request.
     */
    public String getRequestId() {
        return requestId.orElse(NONE);
    }

    /**
     * Gets last successful request's timestamp.
     *
     * @return timestamp associated with last successful request.
     */
    public String getTimestamp() {
        return timestamp.orElse(NONE);
    }

    @Override
    public String toString() {
        return String.format("request id - %s, timestamp - %s", getRequestId(), getTimestamp());
    }

}

