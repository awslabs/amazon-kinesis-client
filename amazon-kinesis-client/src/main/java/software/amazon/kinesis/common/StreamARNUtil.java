package software.amazon.kinesis.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class StreamARNUtil {
    public static Pattern STREAM_ARN_PATTERN = Pattern.compile(
            "arn:aws.*:kinesis:.*:\\d{12}:stream\\/(\\S+)");
    public static Pattern CONSUMER_ARN_PATTERN = Pattern.compile(
            "(arn:aws.*:kinesis:.*:\\d{12}:.*stream\\/[a-zA-Z0-9_.-]+)\\/consumer\\/[a-zA-Z0-9_.-]+:[0-9]+");

    public static String getStreamName(String streamNameOrARN) {
        final Matcher matcher = STREAM_ARN_PATTERN.matcher(streamNameOrARN);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            // Assume that the user entered the stream name because the argument doesn't match the StreamARN regex
            return streamNameOrARN;
        }
    }

    public static Optional<String> getOptionalStreamARN(String streamNameOrARN) {
        final Matcher matcher = STREAM_ARN_PATTERN.matcher(streamNameOrARN);
        if (matcher.find()) {
            return Optional.of(streamNameOrARN);
        } else {
            // Retrieve
            return Optional.empty();
        }
    }

    public static Optional<String> getOptionalStreamARNFromDescribeStreamSummary(
            String streamName, KinesisAsyncClient kinesis) {
        final DescribeStreamSummaryRequest request =
                KinesisRequestsBuilder.describeStreamSummaryRequestBuilder().streamName(streamName).build();
        try {
            DescribeStreamSummaryResponse response = kinesis.describeStreamSummary(request).get();
            return Optional.ofNullable(response.streamDescriptionSummary().streamARN());
        } catch (ExecutionException | InterruptedException e) {
            log.warn("Not able to get StreamARN from the DescribeStreamSummary call", e);
        }
        return Optional.empty();
    }

    public static Optional<String> getOptionalStreamARNFromConsumerARN(String consumerARN) {
        if (StringUtils.isEmpty(consumerARN)) {
            return Optional.empty();
        }

        final Matcher matcher = CONSUMER_ARN_PATTERN.matcher(consumerARN);
        if (matcher.find()) {
            return Optional.ofNullable(matcher.group(1));
        } else {
            log.warn("Can't extract the streamARN from consumerARN \"" + consumerARN +
                    "\", because it doesn't match the regex \"" + CONSUMER_ARN_PATTERN.pattern() + "\"");
            return Optional.empty();
        }
    }

    public static void trySetEmptyStreamARN(StreamIdentifier streamIdentifier, KinesisAsyncClient kinesisAsyncClient) {
        Optional<String> optionalStreamARN = streamIdentifier.streamARN();
        if (!optionalStreamARN.isPresent()) {
            streamIdentifier.streamARN(
                    getOptionalStreamARNFromDescribeStreamSummary(streamIdentifier.streamName(), kinesisAsyncClient));

            log.debug(streamIdentifier.streamARN().isPresent() ?
                    "Successfully set streamARN to " + streamIdentifier.streamARN().get() :
                    "Not able to set streamARN via DescribeStreamSummary call");
        } else {
            log.debug("StreamARN " + optionalStreamARN.get() + " is already passed during initialization.");
        }
    }
}
