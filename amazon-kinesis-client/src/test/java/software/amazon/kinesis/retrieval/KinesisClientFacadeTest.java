package software.amazon.kinesis.retrieval;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.retrieval.KinesisClientFacade.describeStreamSummary;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;

@RunWith(MockitoJUnitRunner.class)
public class KinesisClientFacadeTest {

    @Mock
    private KinesisAsyncClient mockKinesisClient;

    @Before
    public void setUp() {
        KinesisClientFacade.initialize(mockKinesisClient);
    }

    @Test
    public void testDescribeStreamSummary() {
        final DescribeStreamSummaryResponse expectedResponse = DescribeStreamSummaryResponse.builder().build();
        when(mockKinesisClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final DescribeStreamSummaryResponse actualResponse = describeStreamSummary("narf");
        assertEquals(expectedResponse, actualResponse);

        verify(mockKinesisClient).describeStreamSummary(any(DescribeStreamSummaryRequest.class));
    }

    @Test
    public void testDescribeStreamSummaryRetries() throws Exception {
        final DescribeStreamSummaryResponse expectedResponse = DescribeStreamSummaryResponse.builder().build();
        final CompletableFuture<DescribeStreamSummaryResponse> mockFuture = mock(CompletableFuture.class);
        final TimeoutException timeoutException = new TimeoutException();

        when(mockKinesisClient.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get(anyInt(), any(TimeUnit.class)))
                .thenThrow(timeoutException)
                .thenThrow(timeoutException)
                .thenReturn(expectedResponse);

        final DescribeStreamSummaryResponse actualResponse = describeStreamSummary("retry me plz");
        assertEquals(expectedResponse, actualResponse);

        verify(mockKinesisClient).describeStreamSummary(any(DescribeStreamSummaryRequest.class));
        verify(mockFuture, times(3)).get(anyInt(), any(TimeUnit.class));
    }
}