package software.amazon.kinesis.coordinator.streamInfo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamInfoDAOTest {
    @Mock
    private CoordinatorStateDAO mockCoordinatorStateDAO = mock(CoordinatorStateDAO.class, Mockito.RETURNS_MOCKS);

    @Mock
    private KinesisAsyncClient mockKinesisAsyncClient;

    private static final String ENTITY_TYPE = "STREAM";
    private StreamInfoDAO streamInfoDAO;
    private final String streamId = "streamId-123-ghi";
    private final String streamName = "test-stream";
    private final String streamArn = "arn:aws:kinesis:us-west-2:123456789012:stream/test-stream";
    private final StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance(streamName);

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        streamInfoDAO = new StreamInfoDAO(mockCoordinatorStateDAO, mockKinesisAsyncClient);
    }

    @Test
    public void testCreateStreamInfoWhenValidStreamReturnsTrue() throws Exception {
        DescribeStreamSummaryResponse mockResponse = createMockDescribeResponse();
        CompletableFuture<DescribeStreamSummaryResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(future);

        when(mockCoordinatorStateDAO.createCoordinatorStateIfNotExists(any(StreamInfo.class)))
                .thenReturn(true);

        boolean result = streamInfoDAO.createStreamInfo(streamIdentifier);

        assertTrue(result);
        verify(mockKinesisAsyncClient).describeStreamSummary(any(DescribeStreamSummaryRequest.class));
        verify(mockCoordinatorStateDAO).createCoordinatorStateIfNotExists(any(StreamInfo.class));
    }

    @Test
    public void testCreateStreamInfoWhenDAOCreateFailsReturnsFalse() throws Exception {
        DescribeStreamSummaryResponse mockResponse = createMockDescribeResponse();
        CompletableFuture<DescribeStreamSummaryResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(future);

        when(mockCoordinatorStateDAO.createCoordinatorStateIfNotExists(any(StreamInfo.class)))
                .thenReturn(false);

        boolean result = streamInfoDAO.createStreamInfo(streamIdentifier);

        assertFalse(result);
    }

    @Test(expected = Exception.class)
    public void testCreateStreamInfoWithDAOExceptionThrowsException() throws Exception {
        DescribeStreamSummaryResponse mockResponse = createMockDescribeResponse();
        CompletableFuture<DescribeStreamSummaryResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(future);

        when(mockCoordinatorStateDAO.createCoordinatorStateIfNotExists(any(StreamInfo.class)))
                .thenThrow(new RuntimeException("Test exception"));

        boolean result = streamInfoDAO.createStreamInfo(streamIdentifier);

        assertFalse(result);
    }

    @Test(expected = InvalidStateException.class)
    public void testCreateStreamInfoWithNullStreamIdThrowsInvalidStateException() throws Exception {
        DescribeStreamSummaryResponse mockResponse = DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(
                        StreamDescriptionSummary.builder().streamId(null).build())
                .build();
        CompletableFuture<DescribeStreamSummaryResponse> future = CompletableFuture.completedFuture(mockResponse);

        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(future);

        streamInfoDAO.createStreamInfo(streamIdentifier);
    }

    @Test
    public void testCreateStreamInfoWithLimitExceededExceptionRetriesAndSucceeds() throws Exception {
        CompletableFuture<DescribeStreamSummaryResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(LimitExceededException.builder().build());

        DescribeStreamSummaryResponse mockResponse = createMockDescribeResponse();
        CompletableFuture<DescribeStreamSummaryResponse> successFuture =
                CompletableFuture.completedFuture(mockResponse);

        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(failedFuture)
                .thenReturn(successFuture);

        when(mockCoordinatorStateDAO.createCoordinatorStateIfNotExists(any(StreamInfo.class)))
                .thenReturn(true);

        boolean result = streamInfoDAO.createStreamInfo(streamIdentifier);

        assertTrue(result);
        verify(mockKinesisAsyncClient, times(2)).describeStreamSummary(any(DescribeStreamSummaryRequest.class));
    }

    @Test(expected = DependencyException.class)
    public void testCreateStreamInfoWithProvisionedThroughputExceptionThrowsDependencyException() throws Exception {
        // Setup to always fail with ProvisionedThroughputExceededException
        CompletableFuture<DescribeStreamSummaryResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(
                ProvisionedThroughputExceededException.builder().build());

        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(failedFuture);

        streamInfoDAO.createStreamInfo(streamIdentifier);
    }

    @Test(expected = DependencyException.class)
    public void testCreateStreamInfoWithInterruptedExceptionThrowsDependencyException() throws Exception {
        CompletableFuture<DescribeStreamSummaryResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new InterruptedException("Test interruption"));

        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(future);

        streamInfoDAO.createStreamInfo(streamIdentifier);
    }

    @Test(expected = DependencyException.class)
    public void testCreateStreamInfoWithMaxRetriesExceededThrowsDependencyException() throws Exception {
        CompletableFuture<DescribeStreamSummaryResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(LimitExceededException.builder().build());

        // Return LimitExceededException for all calls
        when(mockKinesisAsyncClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(failedFuture);

        streamInfoDAO.createStreamInfo(streamIdentifier);
    }

    @Test
    public void testGetStreamInfoWhenStreamExistsReturnsStreamInfo() throws Exception {
        Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put("streamId", AttributeValue.builder().s(streamId).build());
        attributes.put("entityType", AttributeValue.builder().s(ENTITY_TYPE).build());
        attributes.put("streamArn", AttributeValue.builder().s(streamArn).build());
        StreamInfo mockState = new StreamInfo(streamName, streamId);
        mockState.setAttributes(attributes);
        when(mockCoordinatorStateDAO.getCoordinatorState(streamName)).thenReturn(mockState);

        StreamInfo result = streamInfoDAO.getStreamInfo(streamName);

        assertEquals(streamName, result.getKey());
        assertEquals(streamId, result.getStreamId());
        verify(mockCoordinatorStateDAO).getCoordinatorState(streamName);
    }

    @Test
    public void testGetStreamInfoWhenStreamNotFoundReturnsNull() throws Exception {
        String key = "non-existent-key";
        when(mockCoordinatorStateDAO.getCoordinatorState(key)).thenReturn(null);
        StreamInfo result = streamInfoDAO.getStreamInfo(key);

        assertNull(result);
        verify(mockCoordinatorStateDAO).getCoordinatorState(key);
    }

    @Test(expected = ProvisionedThroughputException.class)
    public void testGetStreamInfoWithProvisionedThroughputExceptionPropagatesException() throws Exception {
        String key = "test-key";
        when(mockCoordinatorStateDAO.getCoordinatorState(key))
                .thenThrow(new ProvisionedThroughputException("Throughput exceeded", null));

        streamInfoDAO.getStreamInfo(key);
        verify(mockCoordinatorStateDAO).getCoordinatorState(key);
    }

    @Test
    public void testListStreamInfoWhenStreamsExistReturnsStreamList() throws Exception {
        String streamName1 = "stream-id-key1";
        Map<String, AttributeValue> attributes1 = new HashMap<>();
        attributes1.put("streamId", AttributeValue.builder().s("stream1").build());
        attributes1.put("entityType", AttributeValue.builder().s(ENTITY_TYPE).build());
        CoordinatorState state1 = new StreamInfo(streamName1, streamId);
        state1.setAttributes(attributes1);

        String streamName2 = "stream-id-key2";
        Map<String, AttributeValue> attributes2 = new HashMap<>();
        attributes2.put("streamId", AttributeValue.builder().s("stream2").build());
        attributes2.put("entityType", AttributeValue.builder().s(ENTITY_TYPE).build());
        CoordinatorState state2 = new StreamInfo(streamName2, streamId);
        state2.setAttributes(attributes2);

        List<CoordinatorState> states = Arrays.asList(state1, state2);

        when(mockCoordinatorStateDAO.listCoordinatorStateByEntityType(ENTITY_TYPE))
                .thenReturn(states);

        List<StreamInfo> result = streamInfoDAO.listStreamInfo();

        assertEquals(2, result.size());
        assertEquals(streamName1, result.get(0).getKey());
        assertEquals(streamName2, result.get(1).getKey());
        verify(mockCoordinatorStateDAO).listCoordinatorStateByEntityType(ENTITY_TYPE);
    }

    @Test(expected = ProvisionedThroughputException.class)
    public void testListStreamInfoWithProvisionedThroughputExceptionThrowsException() throws Exception {
        when(mockCoordinatorStateDAO.listCoordinatorStateByEntityType(ENTITY_TYPE))
                .thenThrow(new ProvisionedThroughputException("Throughput exceeded", null));

        streamInfoDAO.listStreamInfo();
    }

    @Test(expected = DependencyException.class)
    public void testListStreamInfoWithDependencyExceptionThrowsException() throws Exception {
        when(mockCoordinatorStateDAO.listCoordinatorStateByEntityType(ENTITY_TYPE))
                .thenThrow(new DependencyException("Database error", null));

        streamInfoDAO.listStreamInfo();
    }

    @Test(expected = InvalidStateException.class)
    public void testListStreamInfoWithInvalidStateExceptionThrowsException() throws Exception {
        when(mockCoordinatorStateDAO.listCoordinatorStateByEntityType(ENTITY_TYPE))
                .thenThrow(new InvalidStateException("Table doesn't exist"));

        streamInfoDAO.listStreamInfo();
    }

    @Test
    public void testDeleteStreamInfoWhenStreamExistsReturnsTrue() throws Exception {
        when(mockCoordinatorStateDAO.deleteCoordinatorState(streamName)).thenReturn(true);

        boolean result = streamInfoDAO.deleteStreamInfo(streamName);

        assertTrue(result);
        verify(mockCoordinatorStateDAO).deleteCoordinatorState(streamName);
    }

    @Test
    public void testDeleteStreamInfoWhenDAODeleteFailsReturnsFalse() throws Exception {
        when(mockCoordinatorStateDAO.deleteCoordinatorState(streamName)).thenReturn(false);

        boolean result = streamInfoDAO.deleteStreamInfo(streamName);

        assertFalse(result);
        verify(mockCoordinatorStateDAO).deleteCoordinatorState(streamName);
    }

    @Test(expected = ProvisionedThroughputException.class)
    public void testDeleteStreamInfoWithProvisionedThroughputExceptionThrowsException() throws Exception {
        when(mockCoordinatorStateDAO.deleteCoordinatorState(streamName))
                .thenThrow(new ProvisionedThroughputException("Throughput exceeded", null));

        streamInfoDAO.deleteStreamInfo(streamName);
    }

    @Test(expected = DependencyException.class)
    public void testDeleteStreamInfoWithDependencyExceptionThrowsException() throws Exception {
        when(mockCoordinatorStateDAO.deleteCoordinatorState(streamName))
                .thenThrow(new DependencyException("Database error", null));

        streamInfoDAO.deleteStreamInfo(streamName);
    }

    private DescribeStreamSummaryResponse createMockDescribeResponse() {
        StreamDescriptionSummary summary = StreamDescriptionSummary.builder()
                .streamARN(streamArn)
                .streamId(streamId)
                .build();

        return DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(summary)
                .build();
    }
}
