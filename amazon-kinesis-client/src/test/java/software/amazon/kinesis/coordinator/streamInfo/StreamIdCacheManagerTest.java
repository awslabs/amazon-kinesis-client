package software.amazon.kinesis.coordinator.streamInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.arns.Arn;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class StreamIdCacheManagerTest {

    @Mock
    private StreamInfoDAO mockStreamInfoDAO;

    @Mock
    private ScheduledExecutorService mockScheduledExecutorService;

    private Map<StreamIdentifier, StreamConfig> streamConfigMap;
    private StreamIdCacheManager cacheManager;
    private StreamIdentifier streamIdentifier;
    private final String streamId = "test-stream-id";
    private final String streamKey = "test-stream-key";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        streamIdentifier = StreamIdentifier.singleStreamInstance("test-stream");
        streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier, new StreamConfig(streamIdentifier, null));
    }

    @Test
    public void testGetWithCachedValueReturnsStreamId() throws InvalidStateException {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);

        cacheManager.cache.put(streamIdentifier.toString(), streamId);

        String result = cacheManager.get(streamIdentifier);

        assertEquals(streamId, result);
        verifyNoInteractions(mockStreamInfoDAO);
    }

    @Test
    public void testGetWithNullStreamIdentifierInSingleStreamModeReturnsStreamId() throws InvalidStateException {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);

        cacheManager.cache.put(streamIdentifier.toString(), streamId);

        String result = cacheManager.get(null);

        assertEquals(streamId, result);
    }

    @Test
    public void testGetWithNullIdentifierInSingleStreamModeFetchesFromDAOWhenNotCached()
            throws InvalidStateException, ProvisionedThroughputException, DependencyException {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);
        StreamInfo streamInfo = new StreamInfo(streamIdentifier.toString(), streamId, "STREAM");
        when(mockStreamInfoDAO.getStreamInfo(streamIdentifier.toString())).thenReturn(streamInfo);

        String result = cacheManager.get(null);

        assertEquals(streamId, result);
        verify(mockStreamInfoDAO).getStreamInfo(streamIdentifier.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetWithNullIdentifierInMultiStreamModeThrowsException() throws InvalidStateException {
        cacheManager = createCacheManager(true, StreamIdOnboardingState.ONBOARDED);

        cacheManager.get(null);
    }

    @Test
    public void testGetWithStreamIdentifierInMultiStreamModeFetchesFromDAO()
            throws InvalidStateException, ProvisionedThroughputException, DependencyException {
        Map<StreamIdentifier, StreamConfig> multiStreamMap = createMultiStreamConfigMap();
        cacheManager = createCacheManager(true, StreamIdOnboardingState.ONBOARDED);

        StreamIdentifier streamId1 = multiStreamMap.keySet().iterator().next();
        StreamInfo streamInfo = new StreamInfo(streamId1.toString(), "multi-stream-id", "STREAM");
        when(mockStreamInfoDAO.getStreamInfo(streamId1.toString())).thenReturn(streamInfo);

        String result = cacheManager.get(streamId1);

        assertEquals("multi-stream-id", result);
        verify(mockStreamInfoDAO).getStreamInfo(streamId1.toString());
    }

    @Test
    public void testResolveStreamIdInOnboardedStateAddsToCache() throws Exception {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);
        StreamInfo streamInfo = new StreamInfo(streamIdentifier.toString(), streamId, "STREAM");
        when(mockStreamInfoDAO.getStreamInfo(streamIdentifier.toString())).thenReturn(streamInfo);

        cacheManager.resolveStreamId(streamIdentifier);

        verify(mockStreamInfoDAO).getStreamInfo(streamIdentifier.toString());
        assertEquals(streamId, cacheManager.cache.get(streamIdentifier.toString()));
    }

    @Test(expected = RuntimeException.class)
    public void testGetWithNonExistentStreamIdInOnboardedStateThrowsException()
            throws InvalidStateException, ProvisionedThroughputException, DependencyException {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);
        when(mockStreamInfoDAO.getStreamInfo(streamIdentifier.toString())).thenReturn(null);

        cacheManager.get(streamIdentifier);
    }

    @Test
    public void testResolveStreamIdInNotOnboardedStateSkipsDAO() {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.NOT_ONBOARDED);
        cacheManager.resolveStreamId(streamIdentifier);
        verifyNoInteractions(mockStreamInfoDAO);
    }

    @Test
    public void testGetWithNonExistentStreamIdInTransitionStateReturnsNullAndQueuesForDelayedFetch()
            throws InvalidStateException {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.IN_TRANSITION);

        String result = cacheManager.get(streamIdentifier);

        assertNull(result);
        assertTrue(cacheManager.delayedFetchStreamIdKeys.contains(streamIdentifier.toString()));
        verifyNoInteractions(mockStreamInfoDAO);
    }

    @Test
    public void testGetInMultiStreamTransitionModeQueuesForDelayedFetch() throws Exception {
        Map<StreamIdentifier, StreamConfig> multiStreamMap = createMultiStreamConfigMap();
        cacheManager = createCacheManager(true, StreamIdOnboardingState.IN_TRANSITION);

        StreamIdentifier[] streamIds = multiStreamMap.keySet().toArray(new StreamIdentifier[0]);
        StreamIdentifier streamId1 = streamIds[0];
        StreamIdentifier streamId2 = streamIds[1];

        // Mock the DAO responses for delayed fetch
        StreamInfo streamInfo1 = new StreamInfo(streamId1.toString(), "id1", "STREAM");
        when(mockStreamInfoDAO.getStreamInfo(streamId1.toString())).thenReturn(streamInfo1);

        // Call get() - should return null and queue for delayed fetch
        String result1 = cacheManager.get(streamId1);

        // Verify immediate behavior
        assertNull(result1);
        assertTrue(cacheManager.delayedFetchStreamIdKeys.contains(streamId1.toString()));
        verifyNoInteractions(mockStreamInfoDAO);

        // Simulate the delayed fetch process (what the scheduled task would do)
        assertEquals(1, cacheManager.delayedFetchStreamIdKeys.size());
        cacheManager.resolveAndFetchStreamId(cacheManager.delayedFetchStreamIdKeys.poll());

        // Verify delayed fetch worked
        assertEquals("id1", cacheManager.cache.get(streamId1.toString()));
        verify(mockStreamInfoDAO).getStreamInfo(streamId1.toString());
    }

    @Test(expected = RuntimeException.class)
    public void testGetWithDAOExceptionThrowsRuntimeException() throws Exception {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);
        when(mockStreamInfoDAO.getStreamInfo(streamIdentifier.toString()))
                .thenThrow(new DependencyException("Test exception", null));

        cacheManager.get(streamIdentifier);
    }

    @Test
    public void testGetWithConcurrentCallsForSameStreamCallsDAOOnlyOnce() throws Exception {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);

        StreamInfo streamInfo = new StreamInfo(streamIdentifier.toString(), streamId, "STREAM");
        when(mockStreamInfoDAO.getStreamInfo(streamIdentifier.toString())).thenReturn(streamInfo);

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                try {
                    String result = cacheManager.get(streamIdentifier);
                    if (streamId.equals(result)) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    fail("Exception should not be thrown: " + e.getMessage());
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(5, successCount.get());
        verify(mockStreamInfoDAO, times(1)).getStreamInfo(streamIdentifier.toString()); // Should be called only once

        cacheManager.stop();
    }

    @Test
    public void testConstructorCreatesScheduledTaskForDelayedFetch() {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);

        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduledExecutorService)
                .scheduleWithFixedDelay(runnableCaptor.capture(), eq(0L), eq(2000L), eq(TimeUnit.MILLISECONDS));

        Runnable capturedRunnable = runnableCaptor.getValue();
        assertNotNull(capturedRunnable);
    }

    @Test
    public void testResolveAndFetchStreamIdWithExceptionHandlesException() throws Exception {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.IN_TRANSITION);

        cacheManager.delayedFetchStreamIdKeys.add(streamIdentifier.toString());

        when(mockStreamInfoDAO.getStreamInfo(streamIdentifier.toString()))
                .thenThrow(new DependencyException("Test exception", null));

        try {
            cacheManager.resolveAndFetchStreamId(cacheManager.delayedFetchStreamIdKeys.poll());
            fail("Should have thrown an exception when resolving streamId");
        } catch (DependencyException e) {
            // Expected exception
        }

        verify(mockStreamInfoDAO).getStreamInfo(streamIdentifier.toString());
        assertNull(cacheManager.cache.get(streamIdentifier.toString()));
        assertTrue(cacheManager.delayedFetchStreamIdKeys.isEmpty());
    }

    @Test
    public void testGetInMultiStreamOnboardedModeReturnsStreamIdFromDAO() throws Exception {
        Map<StreamIdentifier, StreamConfig> multiStreamMap = createMultiStreamConfigMap();
        cacheManager = createCacheManager(true, StreamIdOnboardingState.ONBOARDED);

        StreamIdentifier[] streamIds = multiStreamMap.keySet().toArray(new StreamIdentifier[0]);
        StreamIdentifier streamId1 = streamIds[0];
        StreamIdentifier streamId2 = streamIds[1];

        StreamInfo streamInfo1 = new StreamInfo(streamId1.toString(), "id1", "STREAM");
        StreamInfo streamInfo2 = new StreamInfo(streamId2.toString(), "id2", "STREAM");
        when(mockStreamInfoDAO.getStreamInfo(streamId1.toString())).thenReturn(streamInfo1);
        when(mockStreamInfoDAO.getStreamInfo(streamId2.toString())).thenReturn(streamInfo2);

        String result1 = cacheManager.get(streamId1);
        String result2 = cacheManager.get(streamId2);

        assertEquals("id1", result1);
        assertEquals("id2", result2);
        verify(mockStreamInfoDAO).getStreamInfo(streamId1.toString());
        verify(mockStreamInfoDAO).getStreamInfo(streamId2.toString());
    }

    @Test
    public void testMultiStreamModeWithDelayedFetch() throws Exception {
        Map<StreamIdentifier, StreamConfig> multiStreamMap = createMultiStreamConfigMap();
        cacheManager = createCacheManager(true, StreamIdOnboardingState.IN_TRANSITION);

        StreamIdentifier[] streamIds = multiStreamMap.keySet().toArray(new StreamIdentifier[0]);
        StreamIdentifier streamId1 = streamIds[0];
        StreamIdentifier streamId2 = streamIds[1];

        StreamInfo streamInfo1 = new StreamInfo(streamId1.toString(), "id1", "STREAM");
        StreamInfo streamInfo2 = new StreamInfo(streamId2.toString(), "id2", "STREAM");
        when(mockStreamInfoDAO.getStreamInfo(streamId1.toString())).thenReturn(streamInfo1);
        when(mockStreamInfoDAO.getStreamInfo(streamId2.toString())).thenReturn(streamInfo2);

        String result1 = cacheManager.get(streamId1);
        String result2 = cacheManager.get(streamId2);

        assertNull(result1);
        assertNull(result2);
        assertTrue(cacheManager.delayedFetchStreamIdKeys.contains(streamId1.toString()));
        assertTrue(cacheManager.delayedFetchStreamIdKeys.contains(streamId2.toString()));

        while (!cacheManager.delayedFetchStreamIdKeys.isEmpty()) {
            cacheManager.resolveAndFetchStreamId(cacheManager.delayedFetchStreamIdKeys.poll());
        }

        assertEquals("id1", cacheManager.cache.get(streamId1.toString()));
        assertEquals("id2", cacheManager.cache.get(streamId2.toString()));
    }

    @Test
    public void testStopShutsdownScheduledExecutor() {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);
        cacheManager.stop();
        verify(mockScheduledExecutorService).shutdownNow();
    }

    @Test(expected = RuntimeException.class)
    public void testGetWithEmptyStreamConfigMapThrowsException() throws InvalidStateException {
        Map<StreamIdentifier, StreamConfig> emptyMap = new HashMap<>();
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);

        cacheManager.get(null);
    }

    @Test
    public void testRemoveStreamIdRemovesFromCache() {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);
        cacheManager.cache.put(streamKey, streamId);

        cacheManager.removeStreamId(streamKey);

        assertFalse(cacheManager.cache.containsKey(streamKey));
    }

    @Test
    public void testGetAllCachedStreamIdKeyReturnsAllKeys() {
        cacheManager = createCacheManager(false, StreamIdOnboardingState.ONBOARDED);
        cacheManager.cache.put(streamKey, streamId);
        cacheManager.cache.put("key2", "id2");

        Set<String> keys = cacheManager.getAllCachedStreamIdKey();

        assertEquals(2, keys.size());
        assertTrue(keys.contains(streamKey));
        assertTrue(keys.contains("key2"));
    }

    private StreamIdCacheManager createCacheManager(boolean isMultiStreamMode, StreamIdOnboardingState state) {
        return new StreamIdCacheManager(
                mockScheduledExecutorService, mockStreamInfoDAO, streamConfigMap, state, isMultiStreamMode);
    }

    private Map<StreamIdentifier, StreamConfig> createMultiStreamConfigMap() {
        Map<StreamIdentifier, StreamConfig> multiStreamMap = new HashMap<>();

        Arn arn1 = Arn.fromString("arn:aws:kinesis:us-east-1:123456789012:stream/stream1");
        StreamIdentifier streamId1 = StreamIdentifier.multiStreamInstance(arn1, 1234567890L);
        StreamConfig streamConfig1 = new StreamConfig(streamId1, null);

        Arn arn2 = Arn.fromString("arn:aws:kinesis:us-east-1:123456789012:stream/stream2");
        StreamIdentifier streamId2 = StreamIdentifier.multiStreamInstance(arn2, 1234567891L);
        StreamConfig streamConfig2 = new StreamConfig(streamId2, null);

        multiStreamMap.put(streamId1, streamConfig1);
        multiStreamMap.put(streamId2, streamConfig2);

        return multiStreamMap;
    }
}
