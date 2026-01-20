package software.amazon.kinesis.coordinator.streamInfo;

/**
 * Test utility for managing StreamIdCache singleton state between tests.
 */
public class StreamIdCacheTestUtil {

    /**
     * Manually reset the StreamIdCache singleton.
     * Call this in @Before or @After methods if not using the JUnit rule.
     */
    public static void resetStreamIdCache() {
        StreamIdCache.reset();
    }

    /**
     * Initialize the StreamIdCache with test values.
     */
    public static void initializeForTest(StreamIdCacheManager cacheManager, StreamIdOnboardingState state) {
        StreamIdCache.reset();
        StreamIdCache.initialize(cacheManager, state);
    }
}
