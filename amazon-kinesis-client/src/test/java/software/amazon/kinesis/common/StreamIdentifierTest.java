package software.amazon.kinesis.common;

import org.junit.Assert;
import org.junit.Before;
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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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

    @Before
    public void setUp() {
        mockStatic(StreamARNUtil.class);

        when(getStreamARN(anyString(), any(Region.class))).thenReturn(Optional.empty());
        when(getStreamARN(STREAM_NAME, KINESIS_REGION)).thenReturn(Optional.of(DEFAULT_ARN));
        when(getStreamARN(STREAM_NAME, KINESIS_REGION, Optional.of(TEST_ACCOUNT_ID)))
                .thenReturn(Optional.of(DEFAULT_ARN));
    }

    /**
     * Test patterns that should match a serialization regex.
     */
    @Test
    public void testMultiStreamDeserializationSuccess() {
        for (final String pattern : Arrays.asList(
                // arn examples
                toArn(KINESIS_REGION).toString(),
                // serialization examples
                "123456789012:stream-name:123",
                "123456789012:stream-name:123:" + Region.US_ISOB_EAST_1
        )) {
            final StreamIdentifier si = StreamIdentifier.multiStreamInstance(pattern);
            assertNotNull(si);
        }
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
                "123456789012:stream-name", // missing delimiter before creation epoch
                "accountId:stream-name:123", // non-numeric account id
//                "123456789:stream-name:123", // account id not 12 digits
                "123456789abc:stream-name:123", // 12char alphanumeric account id
                "123456789012::123", // missing stream name
                "123456789012:stream-name:", // missing creation epoch
                "123456789012:stream-name::", // missing creation epoch; ':' for optional region yet missing region
                "123456789012:stream-name::us-east-1", // missing creation epoch
                "123456789012:stream-name:abc", // non-numeric creation epoch
                "123456789012:stream-name:abc:", // non-numeric creation epoch with ':' yet missing region
                "123456789012:stream-name:123:", // ':' for optional region yet missing region
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
        assertEquals(Optional.of(TEST_ACCOUNT_ID), single.accountIdOptional());
        assertEquals(STREAM_NAME, single.streamName());
        assertEquals(Optional.of(arn), single.streamARNOptional());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInstanceWithoutEpochOrArn() {
        when(getStreamARN(STREAM_NAME, KINESIS_REGION, Optional.of(TEST_ACCOUNT_ID)))
                .thenReturn(Optional.empty());

        final Arn arn = toArn(KINESIS_REGION);
        StreamIdentifier.singleStreamInstance(arn.toString());
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
        assertEquals(Optional.of(DEFAULT_ARN), actualStreamIdentifier.streamARNOptional());
    }

    @Test
    public void testMultiStreamInstanceWithIdentifierSerialization() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(serialize(KINESIS_REGION));
        assertActualStreamIdentifierExpected(actualStreamIdentifier);
    }

    @Test
    public void testMultiStreamInstanceWithRegionSerialized() {
        Region serializedRegion = Region.US_GOV_EAST_1;
        final Optional<Arn> arn = Optional.of(toArn(serializedRegion));

        when(getStreamARN(STREAM_NAME, serializedRegion, Optional.of(TEST_ACCOUNT_ID))).thenReturn(arn);

        final String expectedSerialization = serialize(serializedRegion);
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                expectedSerialization, KINESIS_REGION);
        assertActualStreamIdentifierExpected(arn, actualStreamIdentifier);
        assertEquals(expectedSerialization, actualStreamIdentifier.serialize());
        verifyStatic(StreamARNUtil.class);
        getStreamARN(STREAM_NAME, serializedRegion, Optional.of(TEST_ACCOUNT_ID));
    }

    @Test
    public void testMultiStreamInstanceWithoutRegionSerialized() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                serialize(null), KINESIS_REGION);
        assertActualStreamIdentifierExpected(actualStreamIdentifier);
    }

    private void assertActualStreamIdentifierExpected(StreamIdentifier actual) {
        assertActualStreamIdentifierExpected(Optional.of(DEFAULT_ARN), actual);
    }

    private void assertActualStreamIdentifierExpected(Optional<Arn> expectedArn, StreamIdentifier actual) {
        assertEquals(STREAM_NAME, actual.streamName());
        assertEquals(Optional.of(EPOCH), actual.streamCreationEpochOptional());
        assertEquals(Optional.of(TEST_ACCOUNT_ID), actual.accountIdOptional());
        assertEquals(expectedArn, actual.streamARNOptional());
    }

    /**
     * Creates a pattern that matches {@link StreamIdentifier} serialization.
     *
     * @param region (optional) region to serialize
     */
    private static String serialize(final Region region) {
        return String.join(":", TEST_ACCOUNT_ID, STREAM_NAME, Long.toString(EPOCH)) +
                ((region == null) ? "" : ':' + region.toString());
    }

    private static Arn toArn(final Region region) {
        return Arn.builder().partition("aws").service("kinesis")
                .accountId(TEST_ACCOUNT_ID)
                .resource("stream/" + STREAM_NAME)
                .region(region.toString())
                .build();
    }
}
