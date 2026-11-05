package software.amazon.kinesis.segmenting;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Thrown by {@link FleetSegmentingHandler} when the leader lock's version state cannot be read and the handler has no
 * previously read value to fall back on. Callers must treat this as "no decision possible" and must not contend for or
 * act on either leader lock, because guessing a key is what lets a transient read error flip the fleet's algorithm.
 */
@KinesisClientInternalApi
public class VersionStateUnavailableException extends RuntimeException {
    public VersionStateUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
