package software.amazon.kinesis.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ StreamARNUtil.class })
public class StreamARNUtilTest {
    private static final String STS_RESPONSE_ARN_FORMAT = "arn:aws:sts::%s:assumed-role/Admin/alias";
    private static final String KINESIS_STREAM_ARN_FORMAT = "arn:aws:kinesis:us-east-1:%s:stream/%s";
    // To prevent clashes in the stream arn cache with identical names,
    // we're using the test name as the stream name (key)
    private static final Supplier<String> streamNameProvider = () -> Thread.currentThread().getStackTrace()[2].getMethodName();

    @Mock
    private StsClient mockStsClient;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.spy(StreamARNUtil.class);
        PowerMockito.doReturn(mockStsClient).when(StreamARNUtil.class, "getStsClient");
    }

    @Test
    public void testGetStreamARNHappyCase() {
        String streamName = streamNameProvider.get();
        String accountId = "123456789012";
        when(mockStsClient.getCallerIdentity())
                .thenReturn(GetCallerIdentityResponse.builder().arn(String.format(STS_RESPONSE_ARN_FORMAT, accountId)).build());

        Optional<Arn> actualStreamARNOptional = StreamARNUtil.getStreamARN(streamName, Region.US_EAST_1);
        String expectedStreamARN = String.format(KINESIS_STREAM_ARN_FORMAT, accountId, streamName);
        verify(mockStsClient, times(1)).getCallerIdentity();
        assertTrue(actualStreamARNOptional.isPresent());
        assertEquals(expectedStreamARN, actualStreamARNOptional.get().toString());
    }

    @Test
    public void testGetStreamARNFromCache() {
        String streamName = streamNameProvider.get();
        String accountId = "123456789012";
        when(mockStsClient.getCallerIdentity())
                .thenReturn(GetCallerIdentityResponse.builder().arn(String.format(STS_RESPONSE_ARN_FORMAT, accountId)).build());

        Optional<Arn> actualStreamARNOptional1 = StreamARNUtil.getStreamARN(streamName, Region.US_EAST_1);
        Optional<Arn> actualStreamARNOptional2 = StreamARNUtil.getStreamARN(streamName, Region.US_EAST_1);
        String expectedStreamARN = String.format(KINESIS_STREAM_ARN_FORMAT, accountId, streamName);
        // Since the second ARN is obtained from the cache, hence there's only one sts call
        verify(mockStsClient, times(1)).getCallerIdentity();
        assertEquals(expectedStreamARN, actualStreamARNOptional1.get().toString());
        assertEquals(actualStreamARNOptional1, actualStreamARNOptional2);
    }

    @Test
    public void testGetStreamARNReturnsEmptyOnSTSError() {
        // Optional.empty() is expected when there is an error with the STS call and STS returns empty Arn
        String streamName = streamNameProvider.get();
        when(mockStsClient.getCallerIdentity())
                .thenThrow(AwsServiceException.builder().message("testAwsServiceException").build())
                .thenThrow(SdkClientException.builder().message("testSdkClientException").build());
        assertFalse(StreamARNUtil.getStreamARN(streamName, Region.US_EAST_1).isPresent());
        assertFalse(StreamARNUtil.getStreamARN(streamName, Region.US_EAST_1).isPresent());
    }

    @Test
    public void testGetStreamARNReturnsEmptyOnInvalidKinesisRegion() {
        // Optional.empty() is expected when kinesis region is not set correctly
        String streamName = streamNameProvider.get();
        Optional<Arn> actualStreamARNOptional = StreamARNUtil.getStreamARN(streamName, null);
        verify(mockStsClient, times(0)).getCallerIdentity();
        assertFalse(actualStreamARNOptional.isPresent());
    }

    @Test
    public void testGetStreamARNWithProvidedAccountIDAndIgnoredSTSResult() throws Exception {
        // If the account id is provided in the StreamIdentifier, it will override the result (account id) returned by sts
        String streamName = streamNameProvider.get();
        String stsAccountId = "111111111111";
        String providedAccountId = "222222222222";
        when(mockStsClient.getCallerIdentity())
                .thenReturn(GetCallerIdentityResponse.builder().arn(String.format(STS_RESPONSE_ARN_FORMAT, stsAccountId)).build());

        Optional<Arn> actualStreamARNOptional = StreamARNUtil.getStreamARN(streamName, Region.US_EAST_1, Optional.of(providedAccountId));
        String expectedStreamARN = String.format(KINESIS_STREAM_ARN_FORMAT, providedAccountId, streamName);
        verify(mockStsClient, times(1)).getCallerIdentity();
        assertTrue(actualStreamARNOptional.isPresent());
        assertEquals(expectedStreamARN, actualStreamARNOptional.get().toString());
    }

}
