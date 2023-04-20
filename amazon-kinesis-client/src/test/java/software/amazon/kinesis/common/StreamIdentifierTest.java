package software.amazon.kinesis.common;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static software.amazon.kinesis.common.StreamARNUtil.getStreamARN;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StreamARNUtil.class)
public class StreamIdentifierTest {
    private static final String STREAM_NAME = "stream-name";
    private static final Region KINESIS_REGION = Region.US_WEST_1;
    private static final String TEST_ACCOUNT_ID = "123456789012";
    private static final long EPOCH = 1680616058L;

    private static final Arn DEFAULT_ARN = toArn(KINESIS_REGION);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        StreamARNUtilTest.setUpSupplierCache(new SupplierCache<>(() -> DEFAULT_ARN));
    }

    /**
     * Test patterns that should match a serialization regex.
     */
    @Test
    public void testMultiStreamDeserializationSuccess() {
        final StreamIdentifier siSerialized = StreamIdentifier.multiStreamInstance(serialize());
        assertEquals(Optional.of(EPOCH), siSerialized.streamCreationEpochOptional());
        assertActualStreamIdentifierExpected(null, siSerialized);
    }

    /**
     * Test patterns that <b>should not</b> match a serialization regex.
     */
    @Test
    public void testMultiStreamDeserializationFail() {
        for (final String pattern : Arrays.asList(
                // arn examples
                "arn:aws:kinesis::123456789012:stream/stream-name", // missing region
                "arn:aws:kinesis:region::stream/stream-name", // missing account id
                "arn:aws:kinesis:region:123456789:stream/stream-name", // account id not 12 digits
                "arn:aws:kinesis:region:123456789abc:stream/stream-name", // 12char alphanumeric account id
                "arn:aws:kinesis:region:123456789012:stream/", // missing stream-name
                // serialization examples
                ":stream-name:123", // missing account id
//                "123456789:stream-name:123", // account id not 12 digits
                "123456789abc:stream-name:123", // 12char alphanumeric account id
                "123456789012::123", // missing stream name
                "123456789012:stream-name", // missing delimiter and creation epoch
                "123456789012:stream-name:", // missing creation epoch
                "123456789012:stream-name:abc", // non-numeric creation epoch
                ""
        )) {
            try {
                StreamIdentifier.multiStreamInstance(pattern);
                Assert.fail(pattern + " should not have created a StreamIdentifier");
            } catch (final IllegalArgumentException iae) {
                // expected; ignore
            }
        }
    }

    @Test
    public void testInstanceFromArn() {
        final Arn arn = toArn(KINESIS_REGION);
        final StreamIdentifier single = StreamIdentifier.singleStreamInstance(arn.toString());
        final StreamIdentifier multi = StreamIdentifier.multiStreamInstance(arn.toString());

        assertEquals(single, multi);
        assertEquals(Optional.empty(), single.streamCreationEpochOptional());
        assertActualStreamIdentifierExpected(arn, single);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInstanceWithoutEpochOrArn() {
        mockStatic(StreamARNUtil.class);
        when(getStreamARN(STREAM_NAME, KINESIS_REGION, TEST_ACCOUNT_ID))
                .thenReturn(Optional.empty());

        try {
            StreamIdentifier.singleStreamInstance(DEFAULT_ARN.toString());
        } finally {
            verifyStatic(StreamARNUtil.class);
            getStreamARN(STREAM_NAME, KINESIS_REGION, TEST_ACCOUNT_ID);
        }
    }

    @Test
    public void testSingleStreamInstanceWithName() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(STREAM_NAME);
        assertFalse(actualStreamIdentifier.streamCreationEpochOptional().isPresent());
        assertFalse(actualStreamIdentifier.accountIdOptional().isPresent());
        assertFalse(actualStreamIdentifier.streamARNOptional().isPresent());
        assertEquals(STREAM_NAME, actualStreamIdentifier.streamName());
    }

    @Test
    public void testSingleStreamInstanceWithNameAndRegion() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(STREAM_NAME, KINESIS_REGION);
        assertFalse(actualStreamIdentifier.streamCreationEpochOptional().isPresent());
        assertFalse(actualStreamIdentifier.accountIdOptional().isPresent());
        assertEquals(STREAM_NAME, actualStreamIdentifier.streamName());
        assertEquals(Optional.of(DEFAULT_ARN), actualStreamIdentifier.streamARNOptional());
    }

    @Test
    public void testMultiStreamInstanceWithIdentifierSerialization() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(serialize());
        assertActualStreamIdentifierExpected(null, actualStreamIdentifier);
        assertEquals(Optional.of(EPOCH), actualStreamIdentifier.streamCreationEpochOptional());
    }

    /**
     * When KCL's Kinesis endpoint is a region, it lacks visibility to streams
     * in other regions. Therefore, when the endpoint and ARN conflict, an
     * Exception should be thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testConflictOnRegions() {
        final Region arnRegion = Region.US_GOV_EAST_1;
        assertNotEquals(arnRegion, KINESIS_REGION);

        StreamIdentifier.multiStreamInstance(toArn(arnRegion).toString(), KINESIS_REGION);
    }

    @Test
    public void testMultiStreamInstanceWithoutRegionSerialized() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                serialize(), KINESIS_REGION);
        assertActualStreamIdentifierExpected(actualStreamIdentifier);
    }

    private void assertActualStreamIdentifierExpected(StreamIdentifier actual) {
        assertActualStreamIdentifierExpected(DEFAULT_ARN, actual);
    }

    private void assertActualStreamIdentifierExpected(Arn expectedArn, StreamIdentifier actual) {
        assertEquals(STREAM_NAME, actual.streamName());
        assertEquals(Optional.of(TEST_ACCOUNT_ID), actual.accountIdOptional());
        assertEquals(Optional.ofNullable(expectedArn), actual.streamARNOptional());
    }

    /**
     * Creates a pattern that matches {@link StreamIdentifier} serialization.
     */
    private static String serialize() {
        return String.join(":", TEST_ACCOUNT_ID, STREAM_NAME, Long.toString(EPOCH));
    }

    private static Arn toArn(final Region region) {
        return Arn.builder().partition("aws").service("kinesis")
                .accountId(TEST_ACCOUNT_ID)
                .resource("stream/" + STREAM_NAME)
                .region(region.toString())
                .build();
    }
}
