package software.amazon.kinesis.coordinator.streamInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StreamIdCacheTest {

    @Mock
    private StreamIdCacheManager mockCacheManager;

    @Mock
    private StreamInfoDAO mockStreamInfoDAO;

    @Mock
    private ScheduledExecutorService mockScheduledExecutorService;

    private StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance("test-stream");

    private static final String STREAM_ID = "stream-id-123";
    Map<StreamIdentifier, StreamConfig> streamConfigMap;

    @Before
    public void setUp() {
        StreamIdCacheTestUtil.resetStreamIdCache();
    }

    @Test
    public void testGetInstanceWhenNotInitialized() {
        assertThrows(IllegalStateException.class, StreamIdCache::getInstance);
    }

    @Test
    public void testSingletonBehavior() {
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.ONBOARDED);

        StreamIdCache instance1 = StreamIdCache.getInstance();
        StreamIdCache instance2 = StreamIdCache.getInstance();

        assertSame(instance1, instance2);
    }

    @Test
    public void testInitializeOnlyOnce() {
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.ONBOARDED);
        StreamIdCacheManager secondManager = mock(StreamIdCacheManager.class);

        // try to reinitialize without resetting
        StreamIdCache.initialize(secondManager, StreamIdOnboardingState.IN_TRANSITION);

        assertEquals(
                StreamIdOnboardingState.ONBOARDED, StreamIdCache.getInstance().getOnboardingState());
    }

    @Test
    public void testInitializeAndGetInstance() {
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.ONBOARDED);

        assertDoesNotThrow(StreamIdCache::getInstance);
        assertNotNull(StreamIdCache.getInstance());
        assertEquals(
                StreamIdOnboardingState.ONBOARDED, StreamIdCache.getInstance().getOnboardingState());
    }

    @Test
    public void testGetWhenNotOnboarded() {
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.NOT_ONBOARDED);

        String result = StreamIdCache.get(streamIdentifier);

        assertNull(result);
        verifyNoInteractions(mockCacheManager);
    }

    @Test
    public void testGetWhenOnboarded() throws InvalidStateException {
        when(mockCacheManager.get(streamIdentifier)).thenReturn(STREAM_ID);
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.ONBOARDED);

        String result = StreamIdCache.get(streamIdentifier);

        assertEquals(STREAM_ID, result);
        verify(mockCacheManager).get(streamIdentifier);
    }

    @Test
    public void testGetWhenOnboardedReturnsNull() throws InvalidStateException {
        when(mockCacheManager.get(streamIdentifier)).thenReturn(null);
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.ONBOARDED);

        String result = StreamIdCache.get(streamIdentifier);

        assertNull(result);
        verify(mockCacheManager).get(streamIdentifier);
    }

    @Test
    public void testGetWhenInTransition() throws InvalidStateException {
        when(mockCacheManager.get(streamIdentifier)).thenReturn(STREAM_ID);
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.IN_TRANSITION);

        String result = StreamIdCache.get(streamIdentifier);

        assertEquals(STREAM_ID, result);
        verify(mockCacheManager).get(streamIdentifier);
    }

    @Test
    public void testGetWhenInTransitionReturnsNull() throws InvalidStateException {
        when(mockCacheManager.get(streamIdentifier)).thenReturn(null);
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.IN_TRANSITION);

        String result = StreamIdCache.get(streamIdentifier);

        assertNull(result);
        verify(mockCacheManager).get(streamIdentifier);
    }

    @Test
    public void testGetWithExceptionWhenOnboarded() throws InvalidStateException {
        when(mockCacheManager.get(streamIdentifier)).thenThrow(new RuntimeException("Cache error"));
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.ONBOARDED);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> StreamIdCache.get(streamIdentifier));

        assertEquals("Cache error", exception.getCause().getMessage());
    }

    @Test
    public void testGetWithExceptionWhenInTransition() throws InvalidStateException {
        when(mockCacheManager.get(streamIdentifier)).thenThrow(new RuntimeException("Cache error"));
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.IN_TRANSITION);

        String result = StreamIdCache.get(streamIdentifier);

        assertNull(result);
        verify(mockCacheManager).get(streamIdentifier);
    }

    @Test
    public void testGetWithNullStreamIdentifier() throws InvalidStateException {
        when(mockCacheManager.get(null)).thenReturn(STREAM_ID);
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.ONBOARDED);

        String result = StreamIdCache.get();

        assertEquals(STREAM_ID, result);
        verify(mockCacheManager).get(null);
    }

    @Test
    public void testGetWithNullStreamIdentifierWhenNotOnboarded() {
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.NOT_ONBOARDED);

        String result = StreamIdCache.get();

        assertNull(result);
        verifyNoInteractions(mockCacheManager);
    }

    @Test
    public void testGetWithNullStreamIdentifierWithMultistreamWhenNotOnboarded() {
        StreamIdCacheTestUtil.initializeForTest(mockCacheManager, StreamIdOnboardingState.NOT_ONBOARDED);

        String result = StreamIdCache.get();

        assertNull(result);
        verifyNoInteractions(mockCacheManager);
    }

    @Test
    public void testGetWithNullStreamIdentifierWithMultistreamWhenInTransition() {
        streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier, new StreamConfig(streamIdentifier, null));
        StreamIdCacheManager streamIdCacheManager = createCacheManager(true, StreamIdOnboardingState.IN_TRANSITION);

        StreamIdCacheTestUtil.initializeForTest(streamIdCacheManager, StreamIdOnboardingState.IN_TRANSITION);

        String result = StreamIdCache.get();
        assertNull(result);
        verifyNoInteractions(mockCacheManager);
    }

    @Test
    public void testGetWithExceptionWithMultistreamWhenInOnboarded() {
        streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier, new StreamConfig(streamIdentifier, null));
        StreamIdCacheManager streamIdCacheManager = createCacheManager(true, StreamIdOnboardingState.ONBOARDED);

        StreamIdCacheTestUtil.initializeForTest(streamIdCacheManager, StreamIdOnboardingState.ONBOARDED);

        assertThrows(RuntimeException.class, () -> StreamIdCache.get());
    }

    private StreamIdCacheManager createCacheManager(boolean isMultiStreamMode, StreamIdOnboardingState state) {
        return new StreamIdCacheManager(
                mockScheduledExecutorService,
                mockStreamInfoDAO,
                streamConfigMap,
                state,
                isMultiStreamMode,
                new NullMetricsFactory());
    }
}
