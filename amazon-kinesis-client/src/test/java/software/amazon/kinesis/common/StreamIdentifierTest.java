package software.amazon.kinesis.common;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamIdentifierTest {
    private static final String STREAM_NAME = "stream-name";
    private static final Region KINESIS_REGION = Region.US_WEST_1;
    private static final String TEST_ACCOUNT_ID = "123456789012";
    private static final long EPOCH = 1680616058L;
    private static final Arn DEFAULT_ARN = Arn.builder()
            .partition("aws").service("kinesis")
            .accountId(TEST_ACCOUNT_ID)
            .resource("stream/" + STREAM_NAME)
            .region(KINESIS_REGION.toString())
            .build();

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

    /**
     * Test patterns that <b>should not</b> match a valid streamARN regex.
     */
    @Test
    public void testMultiStreamArnFail() {
        for (final String pattern : Arrays.asList(
                "arn:aws:kinesis::123456789012:stream/stream-name", // missing region
                "arn:aws:kinesis:region::stream/stream-name", // missing account id
                "arn:aws:kinesis:region:123456789:stream/stream-name", // account id not 12 digits
                "arn:aws:kinesis:region:123456789abc:stream/stream-name", // 12char alphanumeric account id
                "arn:aws:kinesis:region:123456789012:stream/" // missing stream-name
        )) {
            try {
                StreamIdentifier.multiStreamInstance(Arn.fromString(pattern), EPOCH);
                Assert.fail(pattern + " should not have created a StreamIdentifier");
            } catch (final IllegalArgumentException iae) {
                // expected; ignore
            }
        }
    }

    @Test
    public void testSingleStreamInstanceFromArn() {
        final StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(DEFAULT_ARN);

        assertTrue(actualStreamIdentifier.streamARNOptional().isPresent());
        assertTrue(actualStreamIdentifier.accountIdOptional().isPresent());
        assertFalse(actualStreamIdentifier.streamCreationEpochOptional().isPresent());

        assertActualStreamIdentifierExpected(DEFAULT_ARN, actualStreamIdentifier);
        assertEquals(actualStreamIdentifier.streamName(), actualStreamIdentifier.serialize());
    }

    @Test
    public void testMultiStreamInstanceFromArn() {
        final StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(DEFAULT_ARN, EPOCH);

        assertTrue(actualStreamIdentifier.streamARNOptional().isPresent());
        assertTrue(actualStreamIdentifier.accountIdOptional().isPresent());
        assertTrue(actualStreamIdentifier.streamCreationEpochOptional().isPresent());

        assertActualStreamIdentifierExpected(DEFAULT_ARN, actualStreamIdentifier);
        assertEquals(serialize(), actualStreamIdentifier.serialize());
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
    public void testMultiStreamInstanceWithIdentifierSerialization() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(serialize());
        assertActualStreamIdentifierExpected(null, actualStreamIdentifier);
        assertEquals(Optional.of(EPOCH), actualStreamIdentifier.streamCreationEpochOptional());
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

}
