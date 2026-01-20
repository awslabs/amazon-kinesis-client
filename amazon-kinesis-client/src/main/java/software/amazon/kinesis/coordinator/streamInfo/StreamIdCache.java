package software.amazon.kinesis.coordinator.streamInfo;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamIdentifier;

@Data
@Slf4j
@KinesisClientInternalApi
public class StreamIdCache {

    private static volatile StreamIdCache instance;
    private final StreamIdCacheManager cacheManager;
    private final StreamIdOnboardingState onboardingState;

    private StreamIdCache(StreamIdCacheManager cacheManager, StreamIdOnboardingState onboardingState) {
        this.cacheManager = cacheManager;
        this.onboardingState = onboardingState;
    }

    public static void initialize(StreamIdCacheManager cacheManager, StreamIdOnboardingState onboardingState) {
        if (instance == null) {
            instance = new StreamIdCache(cacheManager, onboardingState);
        }
    }

    public static StreamIdCache getInstance() {
        if (instance == null) {
            throw new IllegalStateException("StreamIdCache has not been initialized");
        }
        return instance;
    }

    /**
     * This method returns streamId for single stream KCL.
     * If streamId is not onboarded, it returns null.
     * If streamId is not found, it returns null.
     * If KCL app is multi-stream mode then there is no way to find the streamId for the given stream,
     * so it returns exception
     * @return streamId or exception
     */
    public static String get() {
        return get(null);
    }

    /**
     * Gets the stream ID for the given stream name.
     * Returns null if:
     * - Mode is DISABLED
     * - Stream ID is not found
     *
     * @param streamIdentifier
     * @return the stream Id or null
     */
    public static String get(StreamIdentifier streamIdentifier) {
        if (instance == null) {
            throw new IllegalStateException("StreamIdCache has not been initialized");
        }
        if (instance.onboardingState == StreamIdOnboardingState.NOT_ONBOARDED) {
            return null;
        }
        try {
            return instance.getCacheManager().get(streamIdentifier);
        } catch (Exception e) {
            if (instance.onboardingState == StreamIdOnboardingState.ONBOARDED) {
                throw new RuntimeException("Error getting stream ID " + streamIdentifier, e);
            }
            log.info("Error getting stream ID but it will be retried {}", streamIdentifier, e);
            return null;
        }
    }

    // For testing purposes only
    @VisibleForTesting
    static void reset() {
        instance = null;
    }
}
