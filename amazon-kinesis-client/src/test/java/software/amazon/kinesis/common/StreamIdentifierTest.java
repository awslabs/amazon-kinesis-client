package software.amazon.kinesis.common;

import com.google.common.base.Joiner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;


@RunWith(PowerMockRunner.class)
@PrepareForTest(StreamARNUtil.class)
public class StreamIdentifierTest {
    private static final String streamName = "streamName";
    private static final Region kinesisRegion = Region.US_WEST_1;
    private static final String accountId = "111111111111";
    private static final String epoch = "1680616058";

    @Test
    public void testSingleStreamInstanceWithName() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(streamName);
        Assert.assertFalse(actualStreamIdentifier.streamCreationEpochOptional().isPresent());
        Assert.assertFalse(actualStreamIdentifier.accountIdOptional().isPresent());
        Assert.assertFalse(actualStreamIdentifier.streamARNOptional().isPresent());
        Assert.assertEquals(streamName, actualStreamIdentifier.streamName());
    }

    @Test
    public void testSingleStreamInstanceWithNameAndRegion() {
        Optional<Arn> arn = Optional.of(Arn.builder().partition("aws").service("kinesis")
                .region(kinesisRegion.toString()).accountId("123").resource("stream/" + streamName).build());
        mockStatic(StreamARNUtil.class);
        when(StreamARNUtil.getStreamARN(eq(streamName), eq(kinesisRegion))).thenReturn(arn);
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(streamName, kinesisRegion);
        Assert.assertFalse(actualStreamIdentifier.streamCreationEpochOptional().isPresent());
        Assert.assertFalse(actualStreamIdentifier.accountIdOptional().isPresent());
        Assert.assertTrue(actualStreamIdentifier.streamARNOptional().isPresent());
        Assert.assertEquals(arn, actualStreamIdentifier.streamARNOptional());
    }

    @Test
    public void testMultiStreamInstanceWithIdentifierSerialization() {
        String epoch = "1680616058";
        Optional<Arn> arn = Optional.ofNullable(Arn.builder().partition("aws").service("kinesis")
                .accountId(accountId).region(kinesisRegion.toString()).resource("stream/" + streamName).build());

        mockStatic(StreamARNUtil.class);
        when(StreamARNUtil.getStreamARN(eq(streamName), any(), any())).thenReturn(arn);
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                Joiner.on(":").join(accountId, streamName, epoch, kinesisRegion));
        assertActualStreamIdentifierExpected(arn, actualStreamIdentifier);
    }

    @Test
    public void testMultiStreamInstanceWithRegionSerialized() {
        Region serializedRegion = Region.US_GOV_EAST_1;
        Optional<Arn> arn = Optional.ofNullable(Arn.builder().partition("aws").service("kinesis")
                .accountId(accountId).region(serializedRegion.toString()).resource("stream/" + streamName).build());

        mockStatic(StreamARNUtil.class);
        when(StreamARNUtil.getStreamARN(eq(streamName), eq(serializedRegion), any())).thenReturn(arn);
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                Joiner.on(":").join(accountId, streamName, epoch, serializedRegion), kinesisRegion);
        assertActualStreamIdentifierExpected(arn, actualStreamIdentifier);
    }

    @Test
    public void testMultiStreamInstanceWithoutRegionSerialized() {
        Optional<Arn> arn = Optional.ofNullable(Arn.builder().partition("aws").service("kinesis")
                .accountId(accountId).region(kinesisRegion.toString()).resource("stream/" + streamName).build());

        mockStatic(StreamARNUtil.class);
        when(StreamARNUtil.getStreamARN(eq(streamName), eq(kinesisRegion), any())).thenReturn(arn);
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                Joiner.on(":").join(accountId, streamName, epoch), kinesisRegion);
        assertActualStreamIdentifierExpected(arn, actualStreamIdentifier);
    }

    private void assertActualStreamIdentifierExpected(Optional<Arn> expectedARN, StreamIdentifier actual) {
        Assert.assertTrue(actual.streamCreationEpochOptional().isPresent());
        Assert.assertTrue(actual.accountIdOptional().isPresent());
        Assert.assertTrue(actual.streamARNOptional().isPresent());
        Assert.assertEquals(expectedARN, actual.streamARNOptional());
    }
}
