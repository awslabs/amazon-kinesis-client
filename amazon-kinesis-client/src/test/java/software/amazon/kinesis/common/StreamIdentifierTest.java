package software.amazon.kinesis.common;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class StreamIdentifierTest {
    private static final String STREAM_NAME = "stream-name";
    private static final String PARTITION = "aws";
    private static final String SERVICE = "kinesis";
    private static final Region KINESIS_REGION = Region.US_WEST_1;
    private static final String TEST_ACCOUNT_ID = "123456789012";
    private static final String RESOURCE = "stream/" + STREAM_NAME;
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
                "123456789012:stream-name:-123", // negative creation epoch
                "123456789012:stream-name:abc", // non-numeric creation epoch
                ""
        )) {
            try {
                StreamIdentifier.multiStreamInstance(pattern);
                Assert.fail("Serialization " + pattern + " should not have created a StreamIdentifier");
            } catch (final IllegalArgumentException iae) {
                // expected; ignore
            }
        }
    }

    /**
     * Test Arns that <b>should not</b> match a valid AWS Kinesis stream Arn.
     */
    @Test
    public void testMultiStreamByArnWithInvalidStreamArnFail() {
        final String region = KINESIS_REGION.id();
        for (final Arn invalidStreamArn : Arrays.asList(
                streamArn("abc", SERVICE,  region, TEST_ACCOUNT_ID, RESOURCE), // invalid partition
                streamArn(PARTITION, "dynamodb",  region, TEST_ACCOUNT_ID, RESOURCE), // incorrect service
                streamArn(PARTITION, SERVICE,  null, TEST_ACCOUNT_ID, RESOURCE), // missing region
                streamArn(PARTITION, SERVICE,  region, null, RESOURCE), // missing account id
                streamArn(PARTITION, SERVICE,  region, "123456789", RESOURCE), // account id not 12 digits
                streamArn(PARTITION, SERVICE,  region, "123456789abc", RESOURCE), // 12char alphanumeric account id
                streamArn(PARTITION, SERVICE,  region, TEST_ACCOUNT_ID, "table/name"), // incorrect resource type
                Arn.fromString("arn:aws:dynamodb:us-east-2:123456789012:table/myDynamoDBTable") // valid Arn for incorrect resource
        )) {
            try {
                StreamIdentifier.multiStreamInstance(invalidStreamArn, EPOCH);
                Assert.fail("Arn " + invalidStreamArn + " should not have created a StreamIdentifier");
            } catch (final IllegalArgumentException iae) {
                // expected; ignore
            }
        }
    }

    @Test
    public void testMultiStreamByArnWithInvalidCreationEpochFail() {
        final Arn streamArn = streamArn(PARTITION, SERVICE,  KINESIS_REGION.id(), TEST_ACCOUNT_ID, RESOURCE);
        for (final long invalidCreationEpoch : Arrays.asList(-123, 0)) {
            try {
                StreamIdentifier.multiStreamInstance(streamArn, invalidCreationEpoch);
                Assert.fail("Creation epoch" + invalidCreationEpoch + " should not have created a StreamIdentifier");
            } catch (final IllegalArgumentException iae) {
                // expected; ignore
            }
        }
    }

    @Test
    public void testSingleStreamInstanceFromArn() {
        final StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(DEFAULT_ARN);

        assertActualStreamIdentifierExpected(DEFAULT_ARN, actualStreamIdentifier);
        assertEquals(Optional.empty(), actualStreamIdentifier.streamCreationEpochOptional());
        assertEquals(actualStreamIdentifier.streamName(), actualStreamIdentifier.serialize());
    }

    @Test
    public void testMultiStreamInstanceFromArn() {
        final StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(DEFAULT_ARN, EPOCH);

        assertActualStreamIdentifierExpected(DEFAULT_ARN, actualStreamIdentifier);
        assertEquals(Optional.of(EPOCH), actualStreamIdentifier.streamCreationEpochOptional());
        assertEquals(serialize(), actualStreamIdentifier.serialize());
    }

    @Test
    public void testSingleStreamInstanceWithName() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(STREAM_NAME);
        assertEquals(Optional.empty(), actualStreamIdentifier.streamCreationEpochOptional().isPresent());
        assertEquals(Optional.empty(), actualStreamIdentifier.accountIdOptional().isPresent());
        assertEquals(Optional.empty(), actualStreamIdentifier.streamArnOptional().isPresent());
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
        assertEquals(Optional.ofNullable(expectedArn), actual.streamArnOptional());
    }

    /**
     * Creates a pattern that matches {@link StreamIdentifier} serialization.
     */
    private static String serialize() {
        return String.join(":", TEST_ACCOUNT_ID, STREAM_NAME, Long.toString(EPOCH));
    }

    private static Arn streamArn(String partition, String service, String region, String account, String resource) {
        return Arn.builder()
                .partition(partition)
                .service(service)
                .region(region)
                .accountId(account)
                .resource(resource)
                .build();
    }

}
