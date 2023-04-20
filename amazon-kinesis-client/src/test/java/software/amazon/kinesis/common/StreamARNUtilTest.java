package software.amazon.kinesis.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.lang.reflect.Field;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ StreamARNUtil.class, StsClient.class, UrlConnectionHttpClient.class })
public class StreamARNUtilTest {
    private static final String STS_RESPONSE_ARN_FORMAT = "arn:aws:sts::%s:assumed-role/Admin/alias";
    private static final String KINESIS_STREAM_ARN_FORMAT = "arn:aws:kinesis:us-east-1:%s:stream/%s";

    /**
     * Original {@link SupplierCache} that is constructed on class load.
     */
    private static final SupplierCache<Arn> ORIGINAL_CACHE = Whitebox.getInternalState(
            StreamARNUtil.class, "CALLER_IDENTITY_ARN");

    private static final String ACCOUNT_ID = "12345";

    private static final String STREAM_NAME = StreamARNUtilTest.class.getSimpleName();

    @Mock
    private StsClientBuilder mockStsClientBuilder;

    @Mock
    private StsClient mockStsClient;

    private SupplierCache<Arn> spySupplierCache;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        spySupplierCache = spy(ORIGINAL_CACHE);
        setUpSupplierCache(spySupplierCache);

        final Arn defaultArn = toArn(STS_RESPONSE_ARN_FORMAT, ACCOUNT_ID);
        doReturn(defaultArn).when(spySupplierCache).get();
    }

    private void setUpSts() {
        PowerMockito.mockStatic(StsClient.class);
        PowerMockito.mockStatic(UrlConnectionHttpClient.class);

        when(UrlConnectionHttpClient.builder()).thenReturn(mock(UrlConnectionHttpClient.Builder.class));
        when(StsClient.builder()).thenReturn(mockStsClientBuilder);
        when(mockStsClientBuilder.httpClient(any(SdkHttpClient.class))).thenReturn(mockStsClientBuilder);
        when(mockStsClientBuilder.build()).thenReturn(mockStsClient);

        // bypass the spy so the Sts clients are called
        when(spySupplierCache.get()).thenCallRealMethod();
    }

    /**
     * Wrap and embed the original {@link SupplierCache} with a spy to avoid
     * one-and-done cache behavior, provide each test precise control over
     * return values, and enable the ability to verify interactions via Mockito.
     */
    static void setUpSupplierCache(final SupplierCache<Arn> cache) throws Exception {
        final Field f = StreamARNUtil.class.getDeclaredField("CALLER_IDENTITY_ARN");
        f.setAccessible(true);
        f.set(null, cache);
        f.setAccessible(false);
    }

    @Test
    public void testGetStreamARNHappyCase() {
        getStreamArn();

        verify(spySupplierCache).get();
    }

    @Test
    public void testGetStreamARNFromCache() {
        final Optional<Arn> actualStreamARNOptional1 = getStreamArn();
        final Optional<Arn> actualStreamARNOptional2 = getStreamArn();

        verify(spySupplierCache, times(2)).get();
        assertEquals(actualStreamARNOptional1, actualStreamARNOptional2);
    }

    @Test
    public void testGetStreamARNReturnsEmptyOnSTSError() {
        setUpSts();

        // Optional.empty() is expected when there is an error with the STS call and STS returns empty Arn
        when(mockStsClient.getCallerIdentity())
                .thenThrow(AwsServiceException.builder().message("testAwsServiceException").build())
                .thenThrow(SdkClientException.builder().message("testSdkClientException").build());

        assertEquals(Optional.empty(), StreamARNUtil.getStreamARN(STREAM_NAME, Region.US_EAST_1));
        assertEquals(Optional.empty(), StreamARNUtil.getStreamARN(STREAM_NAME, Region.US_EAST_1));
        verify(mockStsClient, times(2)).getCallerIdentity();
        verify(spySupplierCache, times(2)).get();
    }

    @Test(expected = IllegalStateException.class)
    public void testStsResponseWithoutAccountId() {
        setUpSts();

        final Arn arnWithoutAccountId = toArn(STS_RESPONSE_ARN_FORMAT, "");
        assertEquals(Optional.empty(), arnWithoutAccountId.accountId());

        final GetCallerIdentityResponse identityResponse = GetCallerIdentityResponse.builder()
                        .arn(arnWithoutAccountId.toString()).build();
        when(mockStsClient.getCallerIdentity()).thenReturn(identityResponse);

        try {
            StreamARNUtil.getStreamARN(STREAM_NAME, Region.US_EAST_1);
        } finally {
            verify(mockStsClient).getCallerIdentity();
        }
    }

    @Test
    public void testGetStreamARNReturnsEmptyOnInvalidKinesisRegion() {
        // Optional.empty() is expected when kinesis region is not set correctly
        Optional<Arn> actualStreamARNOptional = StreamARNUtil.getStreamARN(STREAM_NAME, null);
        assertEquals(Optional.empty(), actualStreamARNOptional);
        verifyZeroInteractions(mockStsClient);
        verifyZeroInteractions(spySupplierCache);
    }

    @Test
    public void testGetStreamARNWithProvidedAccountIDAndIgnoredSTSResult() {
        // If the account id is provided in the StreamIdentifier, it will override the result (account id) returned by sts
        final String cachedAccountId = "111111111111";
        final String providedAccountId = "222222222222";

        final Arn cachedArn = toArn(STS_RESPONSE_ARN_FORMAT, cachedAccountId);
        when(spySupplierCache.get()).thenReturn(cachedArn);

        final Optional<Arn> actualStreamARNOptional = StreamARNUtil.getStreamARN(STREAM_NAME, Region.US_EAST_1,
                providedAccountId);
        final Arn expectedStreamARN = toArn(KINESIS_STREAM_ARN_FORMAT, providedAccountId, STREAM_NAME);

        verify(spySupplierCache).get();
        verifyZeroInteractions(mockStsClient);
        assertTrue(actualStreamARNOptional.isPresent());
        assertEquals(expectedStreamARN, actualStreamARNOptional.get());
    }

    private static Optional<Arn> getStreamArn() {
        final Optional<Arn> actualArn = StreamARNUtil.getStreamARN(STREAM_NAME, Region.US_EAST_1);
        final Arn expectedArn = toArn(KINESIS_STREAM_ARN_FORMAT, ACCOUNT_ID, STREAM_NAME);

        assertTrue(actualArn.isPresent());
        assertEquals(expectedArn, actualArn.get());

        return actualArn;
    }

    private static Arn toArn(final String format, final Object... params) {
        return Arn.fromString(String.format(format, params));
    }

}
