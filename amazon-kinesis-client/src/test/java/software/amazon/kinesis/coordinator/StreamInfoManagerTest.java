package software.amazon.kinesis.coordinator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.streamInfo.StreamIdOnboardingState;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfo;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoDAO;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoMode;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.metrics.MetricsFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class StreamInfoManagerTest {
    private static final StreamIdOnboardingState STREAM_ID_ONBOARDING_STATE = StreamIdOnboardingState.NOT_ONBOARDED;

    @Mock
    private ScheduledExecutorService mockExecutorService;

    @Mock
    private StreamInfoDAO mockStreamInfoDAO;

    @Mock
    private MetricsFactory mockMetricsFactory;

    @Mock
    private CoordinatorStateDAO mockCoordinatorStateDAO;

    @Mock
    private KinesisAsyncClient mockKinesisAsyncClient;

    private Map<StreamIdentifier, StreamConfig> streamConfigMap;
    private StreamInfoManager streamInfoManager;
    private StreamIdentifier streamIdentifier;
    private StreamConfig streamConfig;
    private static final long BACKFILL_INTERVAL = 1000L;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        streamIdentifier = StreamIdentifier.singleStreamInstance("test-stream");
        streamConfig = new StreamConfig(streamIdentifier, null);
        streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier, streamConfig);
    }

    @Test
    public void testStartWithDisabledModeDoesNotStartExecutor() {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.DISABLED);
        streamInfoManager.start();

        verifyNoInteractions(mockExecutorService);
        assertFalse(streamInfoManager.isRunning());
    }

    @Test
    public void testStartWithTrackOnlyModeStartsExecutorAndSchedulesBackfill() {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);
        streamInfoManager.start();
        assertTrue(streamInfoManager.isRunning());
        verify(mockExecutorService)
                .scheduleWithFixedDelay(any(), eq(0L), eq(BACKFILL_INTERVAL), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testStopWithoutShutdownFlagDoesNotShutdownExecutor() {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);
        streamInfoManager.start();
        streamInfoManager.stop(false);

        verify(mockExecutorService, never()).shutdown();
        assertFalse(streamInfoManager.isRunning());
    }

    @Test
    public void testStopWithShutdownFlagShutsdownExecutor() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);
        streamInfoManager.start();
        // Mock the awaitTermination call
        when(mockExecutorService.awaitTermination(anyLong(), any())).thenReturn(true);
        streamInfoManager.stop(true);

        verify(mockExecutorService).shutdown();
        verify(mockExecutorService).awaitTermination(anyLong(), any());
        assertFalse(streamInfoManager.isRunning());
    }

    @Test
    public void testStopWithInterruptedExceptionCallsShutdownNow() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);

        streamInfoManager.start();

        // Mock the awaitTermination call to throw InterruptedException
        when(mockExecutorService.awaitTermination(anyLong(), any()))
                .thenThrow(new InterruptedException("Test interruption"));

        streamInfoManager.stop(true);

        verify(mockExecutorService).shutdown();
        verify(mockExecutorService).awaitTermination(anyLong(), any());
        verify(mockExecutorService).shutdownNow();
        assertFalse(streamInfoManager.isRunning());
    }

    @Test
    public void testCreateStreamInfoWithDisabledModeDoesNotCallDAO() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.DISABLED);

        streamInfoManager.createStreamInfo(streamIdentifier);
        verifyNoInteractions(mockStreamInfoDAO);

        streamInfoManager.deleteStreamInfo(streamIdentifier);
        verifyNoInteractions(mockStreamInfoDAO);
    }

    @Test
    public void testCreateStreamInfoWithTrackOnlyModeCallsDAO() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);
        streamInfoManager.createStreamInfo(streamIdentifier);

        verify(mockStreamInfoDAO).createStreamInfo(streamIdentifier);
    }

    @Test
    public void testCreateStreamInfoWithMultiStreamModeCallsDAO() throws Exception {
        Map<StreamIdentifier, StreamConfig> multiStreamMap = createMultiStreamConfigMap();
        StreamIdentifier multiStreamId = multiStreamMap.keySet().iterator().next();

        streamInfoManager = createStreamInfoManager(multiStreamMap, true, StreamInfoMode.TRACK_ONLY);
        streamInfoManager.createStreamInfo(multiStreamId);

        verify(mockStreamInfoDAO).createStreamInfo(multiStreamId);
    }

    @Test
    public void testCreateStreamInfoWithNullStreamIdThrowsInvalidStateException() throws Exception {
        StreamInfoDAO streamInfoDAO = new StreamInfoDAO(mockCoordinatorStateDAO, mockKinesisAsyncClient);
        StreamInfoManager currentStreamInfoManager = new StreamInfoManager(
                mockExecutorService,
                streamConfigMap,
                streamInfoDAO,
                mockMetricsFactory,
                true,
                BACKFILL_INTERVAL,
                StreamInfoMode.TRACK_ONLY,
                StreamIdOnboardingState.IN_TRANSITION);
        StreamDescriptionSummary summary =
                StreamDescriptionSummary.builder().streamId(null).build();

        DescribeStreamSummaryResponse mockResponse = DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(summary)
                .build();
        CompletableFuture<DescribeStreamSummaryResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(future);

        String expectedExceptionMessage = "Failed to create stream metadata because StreamId response from "
                + "describeStreamSummary call is null or empty for streamIdentifier " + streamIdentifier;
        String actualExceptionMessage = null;
        try {
            currentStreamInfoManager.createStreamInfo(streamIdentifier);
        } catch (InvalidStateException e) {
            actualExceptionMessage = e.getMessage();
        }
        assertEquals(expectedExceptionMessage, actualExceptionMessage);
        verify(mockCoordinatorStateDAO, never()).createCoordinatorStateIfNotExists(any());
    }

    @Test(expected = Exception.class)
    public void testDeleteStreamInfoWithDAOExceptionThrowsException() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);

        when(mockStreamInfoDAO.deleteStreamInfo(streamIdentifier.serialize()))
                .thenThrow(new RuntimeException("Test exception"));

        streamInfoManager.deleteStreamInfo(streamIdentifier);
    }

    @Test
    public void testPerformBackfillWhenAllStreamsExistDoesNotCreateNew() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);

        // Setup existing stream info
        List<StreamInfo> streamInfoList = new ArrayList<>();
        StreamInfo streamInfo = new StreamInfo(streamIdentifier.serialize(), "stream-id", "STREAM");
        streamInfoList.add(streamInfo);
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(streamInfoList);

        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO, never()).createStreamInfo(any(StreamIdentifier.class));
    }

    @Test
    public void testPerformBackfillNoStreamsExistYet() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);

        // Return empty list from listStreamInfo
        List<StreamInfo> emptyStreamInfoList = new ArrayList<>();
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(emptyStreamInfoList);

        // Mock successful creation
        when(mockStreamInfoDAO.createStreamInfo(streamIdentifier)).thenReturn(true);

        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO).createStreamInfo(streamIdentifier);
    }

    @Test
    public void testPerformBackfillMixedExistingAndNewStreams() throws Exception {
        // Create map with two streams
        StreamIdentifier streamId1 = StreamIdentifier.singleStreamInstance("test-stream-1");
        StreamIdentifier streamId2 = StreamIdentifier.singleStreamInstance("test-stream-2");
        Map<StreamIdentifier, StreamConfig> testMap = new HashMap<>();
        testMap.put(streamId1, new StreamConfig(streamId1, null));
        testMap.put(streamId2, new StreamConfig(streamId2, null));

        streamInfoManager = createStreamInfoManager(testMap, false, StreamInfoMode.TRACK_ONLY);

        // Setup one existing stream
        List<StreamInfo> streamInfoList = new ArrayList<>();
        StreamInfo streamInfo = new StreamInfo(streamId1.serialize(), "stream-id-1", "STREAM");
        streamInfoList.add(streamInfo);
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(streamInfoList);

        // Mock successful creation for the second stream
        when(mockStreamInfoDAO.createStreamInfo(streamId2)).thenReturn(true);

        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO, never()).createStreamInfo(streamId1);
        verify(mockStreamInfoDAO).createStreamInfo(streamId2);
    }

    @Test
    public void testPerformBackfillNullStreamConfig() throws Exception {
        // Create map with one stream having null config
        Map<StreamIdentifier, StreamConfig> testMap = new HashMap<>();
        testMap.put(streamIdentifier, null);

        streamInfoManager = createStreamInfoManager(testMap, false, StreamInfoMode.TRACK_ONLY);

        // Return empty list from listStreamInfo
        List<StreamInfo> emptyStreamInfoList = new ArrayList<>();
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(emptyStreamInfoList);

        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO, never()).createStreamInfo(any(StreamIdentifier.class));
    }

    @Test
    public void testPerformBackfillFailedCreationMaxRetriesExceeded() throws Exception {
        int maxRetryAttempt = 3;
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);

        // Return empty list from listStreamInfo
        List<StreamInfo> emptyStreamInfoList = new ArrayList<>();
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(emptyStreamInfoList);

        // Mock failed creation
        when(mockStreamInfoDAO.createStreamInfo(streamIdentifier)).thenReturn(false);

        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO, times(1 + maxRetryAttempt))
                .createStreamInfo(streamIdentifier); // Once in main loop, once in retry
    }

    @Test
    public void testPerformBackfillFailedCreationWithRetrySuccess() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);

        // Return empty list from listStreamInfo
        List<StreamInfo> emptyStreamInfoList = new ArrayList<>();
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(emptyStreamInfoList);

        // Mock failed creation then success
        when(mockStreamInfoDAO.createStreamInfo(streamIdentifier))
                .thenReturn(false) // First call fails
                .thenReturn(true); // Second call succeeds

        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO, times(2)).createStreamInfo(streamIdentifier);
    }

    @Test
    public void testPerformBackfillWithListStreamInfoException() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);

        // Simulate exception when listing stream info
        when(mockStreamInfoDAO.listStreamInfo()).thenThrow(new RuntimeException("Test exception"));
        when(mockStreamInfoDAO.createStreamInfo(streamIdentifier)).thenReturn(true);
        // Use reflection to access and invoke the private performBackfill method
        performBackfill();

        // Verify the expected interactions with mockStreamInfoDAO
        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO).createStreamInfo(streamIdentifier);
    }

    @Test
    public void testPerformBackfillWithMultiStreamMode() throws Exception {
        Map<StreamIdentifier, StreamConfig> multiStreamMap = createMultiStreamConfigMap();
        List<StreamIdentifier> streamIds = new ArrayList<>(multiStreamMap.keySet());
        StreamIdentifier streamId1 = streamIds.get(0);
        StreamIdentifier streamId2 = streamIds.get(1);

        streamInfoManager = createStreamInfoManager(multiStreamMap, true, StreamInfoMode.TRACK_ONLY);

        List<StreamInfo> emptyStreamInfoList = new ArrayList<>();
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(emptyStreamInfoList);

        // Mock successful creation for both streams
        when(mockStreamInfoDAO.createStreamInfo(streamId1)).thenReturn(true);
        when(mockStreamInfoDAO.createStreamInfo(streamId2)).thenReturn(true);
        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO).createStreamInfo(streamId1); // Should create first stream
        verify(mockStreamInfoDAO).createStreamInfo(streamId2); // Should create second stream
    }

    @Test
    public void testPerformBackfillContinuesProcessingAfterException() throws Exception {
        StreamIdentifier streamId1 = createStreamIdentifier("stream1");
        StreamIdentifier streamId2 = createStreamIdentifier("stream2");
        StreamIdentifier streamId3 = createStreamIdentifier("stream3");

        Map<StreamIdentifier, StreamConfig> streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamId1, new StreamConfig(streamId1, null));
        streamConfigMap.put(streamId2, new StreamConfig(streamId2, null));
        streamConfigMap.put(streamId3, new StreamConfig(streamId3, null));

        streamInfoManager = createStreamInfoManager(streamConfigMap, true, StreamInfoMode.TRACK_ONLY);

        // Return empty list from listStreamInfo to simulate no existing data
        List<StreamInfo> emptyStreamInfoList = new ArrayList<>();
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(emptyStreamInfoList);

        when(mockStreamInfoDAO.createStreamInfo(streamId1)).thenReturn(true);
        when(mockStreamInfoDAO.createStreamInfo(streamId2))
                .thenThrow(new DependencyException("Test exception for stream2", null));
        when(mockStreamInfoDAO.createStreamInfo(streamId3)).thenReturn(true);

        performBackfill();

        // Verify that all three streams were processed
        verify(mockStreamInfoDAO).createStreamInfo(streamId1);
        // Once in main loop, once in retry
        verify(mockStreamInfoDAO, times(2)).createStreamInfo(streamId2);
        verify(mockStreamInfoDAO).createStreamInfo(streamId3);
    }

    @Test
    public void testPerformBackfillMultipleExceptions() throws Exception {
        StreamIdentifier streamId1 = createStreamIdentifier("stream1");
        StreamIdentifier streamId2 = createStreamIdentifier("stream2");
        StreamIdentifier streamId3 = createStreamIdentifier("stream3");

        Map<StreamIdentifier, StreamConfig> streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamId1, new StreamConfig(streamId1, null));
        streamConfigMap.put(streamId2, new StreamConfig(streamId2, null));
        streamConfigMap.put(streamId3, new StreamConfig(streamId3, null));

        streamInfoManager = createStreamInfoManager(streamConfigMap, true, StreamInfoMode.TRACK_ONLY);

        List<StreamInfo> emptyStreamInfoList = new ArrayList<>();
        when(mockStreamInfoDAO.listStreamInfo()).thenReturn(emptyStreamInfoList);

        when(mockStreamInfoDAO.createStreamInfo(streamId1))
                .thenThrow(new DependencyException("Test exception for stream1", null));
        when(mockStreamInfoDAO.createStreamInfo(streamId2))
                .thenThrow(new DependencyException("Test exception for stream2", null));
        when(mockStreamInfoDAO.createStreamInfo(streamId3))
                .thenThrow(new DependencyException("Test exception for stream3", null));

        // Execute the performBackfill method
        performBackfill();

        // Verify that all three streams were processed in the main loop and attempted again
        verify(mockStreamInfoDAO, times(2)).createStreamInfo(streamId1);
        verify(mockStreamInfoDAO, times(2)).createStreamInfo(streamId2);
        verify(mockStreamInfoDAO, times(2)).createStreamInfo(streamId3);
    }

    @Test
    public void testPerformBackfillExceptionInListStreamInfo_ContinuesWithRetry() throws Exception {
        StreamIdentifier streamId1 = createStreamIdentifier("stream1");
        StreamIdentifier streamId2 = createStreamIdentifier("stream2");

        Map<StreamIdentifier, StreamConfig> streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamId1, new StreamConfig(streamId1, null));
        streamConfigMap.put(streamId2, new StreamConfig(streamId2, null));

        streamInfoManager = createStreamInfoManager(streamConfigMap, true, StreamInfoMode.TRACK_ONLY);

        // Mock listStreamInfo to throw exception
        when(mockStreamInfoDAO.listStreamInfo()).thenThrow(new RuntimeException("Test exception in listStreamInfo"));
        when(mockStreamInfoDAO.createStreamInfo(any(StreamIdentifier.class))).thenReturn(true);

        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();

        // Verify that both streams were processed in the retry phase
        verify(mockStreamInfoDAO).createStreamInfo(streamId1);
        verify(mockStreamInfoDAO).createStreamInfo(streamId2);
    }

    @Test
    public void testPerformBackfillEmptyStreamConfigMap() throws Exception {
        streamInfoManager = createStreamInfoManager(new HashMap<>(), false, StreamInfoMode.TRACK_ONLY);

        performBackfill();

        verify(mockStreamInfoDAO).listStreamInfo();
        verify(mockStreamInfoDAO, never()).createStreamInfo(any(StreamIdentifier.class));
    }

    @Test
    public void testStopWithAwaitTerminationTimeout() throws Exception {
        streamInfoManager = createStreamInfoManager(streamConfigMap, false, StreamInfoMode.TRACK_ONLY);
        streamInfoManager.start();

        // Mock the awaitTermination call to return false (timeout)
        when(mockExecutorService.awaitTermination(anyLong(), any()))
                .thenReturn(false)
                .thenReturn(false);

        streamInfoManager.stop(true);

        verify(mockExecutorService).shutdown();
        verify(mockExecutorService).shutdownNow();
        verify(mockExecutorService, times(2)).awaitTermination(anyLong(), any());
        assertFalse(streamInfoManager.isRunning());
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

    private StreamInfoManager createStreamInfoManager(
            Map<StreamIdentifier, StreamConfig> configMap, boolean isMultiStreamMode, StreamInfoMode streamInfoMode) {
        return new StreamInfoManager(
                mockExecutorService,
                configMap,
                mockStreamInfoDAO,
                mockMetricsFactory,
                isMultiStreamMode,
                BACKFILL_INTERVAL,
                streamInfoMode,
                STREAM_ID_ONBOARDING_STATE);
    }

    private void performBackfill() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // Use reflection to access and invoke the private performBackfill method
        java.lang.reflect.Method performBackfillMethod = StreamInfoManager.class.getDeclaredMethod("performBackfill");
        performBackfillMethod.setAccessible(true);
        performBackfillMethod.invoke(streamInfoManager);
    }

    private StreamIdentifier createStreamIdentifier(String streamName) {
        Arn arn = Arn.fromString("arn:aws:kinesis:us-east-1:123456789012:stream/" + streamName);
        return StreamIdentifier.multiStreamInstance(arn, System.currentTimeMillis());
    }
}
