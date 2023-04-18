package software.amazon.kinesis.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StreamARNUtil.class)
public class StreamIdentifierTest {
    private static final String STREAM_NAME = "streamName";
    private static final Region KINESIS_REGION = Region.US_WEST_1;
    private static final String TEST_ACCOUNT_ID = "111111111111";
    private static final String EPOCH = "1680616058";

    private static final Arn DEFAULT_ARN = Arn.builder().partition("aws").service("kinesis")
            .region(KINESIS_REGION.toString()).accountId(TEST_ACCOUNT_ID).resource("stream/" + STREAM_NAME).build();

    @Before
    public void setUp() {
        mockStatic(StreamARNUtil.class);

        when(StreamARNUtil.getStreamARN(anyString(), any(Region.class))).thenReturn(Optional.empty());
        when(StreamARNUtil.getStreamARN(STREAM_NAME, KINESIS_REGION)).thenReturn(Optional.of(DEFAULT_ARN));
        when(StreamARNUtil.getStreamARN(STREAM_NAME, KINESIS_REGION, Optional.of(TEST_ACCOUNT_ID)))
                .thenReturn(Optional.of(DEFAULT_ARN));
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
        assertTrue(actualStreamIdentifier.streamARNOptional().isPresent());
        assertEquals(DEFAULT_ARN, actualStreamIdentifier.streamARNOptional().get());
    }

    @Test
    public void testMultiStreamInstanceWithIdentifierSerialization() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                String.join(":", TEST_ACCOUNT_ID, STREAM_NAME, EPOCH, KINESIS_REGION.toString()));
        assertActualStreamIdentifierExpected(DEFAULT_ARN, actualStreamIdentifier);
    }

    @Test
    public void testMultiStreamInstanceWithRegionSerialized() {
        Region serializedRegion = Region.US_GOV_EAST_1;
        Optional<Arn> arn = Optional.ofNullable(Arn.builder().partition("aws").service("kinesis")
                .accountId(TEST_ACCOUNT_ID).region(serializedRegion.toString()).resource("stream/" + STREAM_NAME).build());

        when(StreamARNUtil.getStreamARN(eq(STREAM_NAME), eq(serializedRegion), any(Optional.class))).thenReturn(arn);
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                String.join(":", TEST_ACCOUNT_ID, STREAM_NAME, EPOCH, serializedRegion.toString()), KINESIS_REGION);
        assertActualStreamIdentifierExpected(arn, actualStreamIdentifier);
    }

    @Test
    public void testMultiStreamInstanceWithoutRegionSerialized() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                String.join(":", TEST_ACCOUNT_ID, STREAM_NAME, EPOCH), KINESIS_REGION);
        assertActualStreamIdentifierExpected(DEFAULT_ARN, actualStreamIdentifier);
    }

    private void assertActualStreamIdentifierExpected(Optional<Arn> expectedARN, StreamIdentifier actual) {
        assertActualStreamIdentifierExpected(expectedARN.get(), actual);
    }

    private void assertActualStreamIdentifierExpected(Arn expectedARN, StreamIdentifier actual) {
        assertTrue(actual.streamCreationEpochOptional().isPresent());
        assertTrue(actual.accountIdOptional().isPresent());
        assertTrue(actual.streamARNOptional().isPresent());
        assertEquals(Optional.of(expectedARN), actual.streamARNOptional());
    }
}
