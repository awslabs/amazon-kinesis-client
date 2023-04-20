/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.common;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.utils.Validate;
import software.amazon.kinesis.retrieval.KinesisClientFacade;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Builder(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
@Getter
@Accessors(fluent = true)
public class StreamIdentifier {

    @Builder.Default
    private final Optional<String> accountIdOptional = Optional.empty();
    @NonNull
    private final String streamName;
    @Builder.Default
    private Optional<Long> streamCreationEpochOptional = Optional.empty();
    @Builder.Default
    private final Optional<Arn> streamARNOptional = Optional.empty();

    /**
     * Pattern for a serialized {@link StreamIdentifier}. The valid format is
     * {@code <accountId>:<streamName>:<creationEpoch>}.
     */
    private static final Pattern STREAM_IDENTIFIER_PATTERN = Pattern.compile(
            "(?<accountId>[0-9]+):(?<streamName>[^:]+):(?<creationEpoch>[0-9]+)");

    /**
     * Pattern for a stream ARN. The valid format is
     * {@code arn:aws:kinesis:<region>:<accountId>:stream:<streamName>}
     * where {@code region} is the id representation of a {@link Region}.
     */
    private static final Pattern STREAM_ARN_PATTERN = Pattern.compile(
            "arn:aws:kinesis:(?<region>[-a-z0-9]+):(?<accountId>[0-9]{12}):stream/(?<streamName>.+)");

    /**
     * Serialize the current StreamIdentifier instance.
     *
     * @return a String of {@code account:stream:creationEpoch[:region]}
     *      where {@code region} is the id representation of a {@link Region}
     *      and is optional.
     */
    public String serialize() {
        if (!accountIdOptional.isPresent()) {
            return streamName;
        }

        if (!streamCreationEpochOptional.isPresent()) {
            // FIXME bias-for-action hack to simplify back-porting into KCL 1.x and facilitate the
            //      backwards-compatible requirement. There's a chicken-and-egg issue if DSS is
            //      called as the application is being configured (and before the client is rigged).
            //      Furthermore, if epoch isn't lazy-loaded here, the problem quickly spirals into
            //      systemic issues of concurrency and consistency (e.g., PeriodicShardSyncManager,
            //      Scheduler, DDB leases). We should look at leveraging dependency injection.
            //      (NOTE: not to inject the Kinesis client here, but to ensure the client is
            //      accessible elsewhere ASAP.)
            final DescribeStreamSummaryResponse dss = KinesisClientFacade.describeStreamSummary(
                    streamARNOptional().get().toString());
            final long creationEpoch = dss.streamDescriptionSummary().streamCreationTimestamp().getEpochSecond();
            streamCreationEpochOptional = Optional.of(creationEpoch);
        }

        final char delimiter = ':';
        final StringBuilder sb = new StringBuilder(accountIdOptional.get()).append(delimiter)
                .append(streamName).append(delimiter);
        streamCreationEpochOptional.ifPresent(sb::append);
        return sb.toString();
    }

    @Override
    public String toString() {
        return serialize();
    }

    /**
     * Create a multi stream instance for StreamIdentifier from serialized stream identifier.
     *
     * @param serializationOrArn serialized {@link StreamIdentifier} or AWS ARN of a Kinesis stream
     *
     * @see #multiStreamInstance(String, Region)
     * @see #serialize()
     */
    public static StreamIdentifier multiStreamInstance(String serializationOrArn) {
        return multiStreamInstance(serializationOrArn, null);
    }

    /**
     * Create a multi stream instance for StreamIdentifier from serialized stream identifier.
     *
     * @param serializationOrArn serialized {@link StreamIdentifier} or AWS ARN of a Kinesis stream
     * @param kinesisRegion Kinesis client endpoint, and also where the stream(s) to be
     *          processed are located. A null will default to the caller's region.
     *
     * @see #multiStreamInstance(String)
     * @see #serialize()
     */
    public static StreamIdentifier multiStreamInstance(String serializationOrArn, Region kinesisRegion) {
        final StreamIdentifier fromSerialization = fromSerialization(serializationOrArn, kinesisRegion);
        if (fromSerialization != null) {
            return fromSerialization;
        }
        final StreamIdentifier fromArn = fromArn(serializationOrArn, kinesisRegion);
        if (fromArn != null) {
            return fromArn;
        }

        throw new IllegalArgumentException("Unable to deserialize StreamIdentifier from " + serializationOrArn);
    }

    /**
     * Create a single stream instance for StreamIdentifier from stream name.
     *
     * @param streamNameOrArn stream name or AWS ARN of a Kinesis stream
     *
     * @see #singleStreamInstance(String, Region)
     */
    public static StreamIdentifier singleStreamInstance(String streamNameOrArn) {
        return singleStreamInstance(streamNameOrArn, null);
    }

    /**
     * Create a single stream instance for StreamIdentifier from the provided stream name and kinesisRegion.
     * This method also constructs the optional StreamARN based on the region info.
     *
     * @param streamNameOrArn stream name or AWS ARN of a Kinesis stream
     * @param kinesisRegion Kinesis client endpoint, and also where the stream(s) to be
     *          processed are located. A null will default to the caller's region.
     *
     * @see #singleStreamInstance(String)
     */
    public static StreamIdentifier singleStreamInstance(String streamNameOrArn, Region kinesisRegion) {
        Validate.notEmpty(streamNameOrArn, "StreamName should not be empty");

        final StreamIdentifier fromArn = fromArn(streamNameOrArn, kinesisRegion);
        if (fromArn != null) {
            return fromArn;
        }

        return StreamIdentifier.builder()
                .streamName(streamNameOrArn)
                .streamARNOptional(StreamARNUtil.getStreamARN(streamNameOrArn, kinesisRegion))
                .build();
    }

    /**
     * Deserializes a StreamIdentifier from {@link #STREAM_IDENTIFIER_PATTERN}.
     *
     * @param input input string (e.g., ARN, serialized instance) to convert into an instance
     * @param kinesisRegion Kinesis client endpoint, and also where the stream(s) to be
     *          processed are located. A null will default to the caller's region.
     * @return a StreamIdentifier instance if the pattern matched, otherwise null
     */
    private static StreamIdentifier fromSerialization(final String input, final Region kinesisRegion) {
        final Matcher matcher = STREAM_IDENTIFIER_PATTERN.matcher(input);
        return matcher.matches()
                ? toStreamIdentifier(matcher, matcher.group("creationEpoch"), kinesisRegion) : null;
    }

    /**
     * Constructs a StreamIdentifier from {@link #STREAM_ARN_PATTERN}.
     *
     * @param input input string (e.g., ARN, serialized instance) to convert into an instance
     * @param kinesisRegion Kinesis client endpoint, and also where the stream(s) to be
     *          processed are located. A null will default to the caller's region.
     * @return a StreamIdentifier instance if the pattern matched, otherwise null
     */
    private static StreamIdentifier fromArn(final String input, final Region kinesisRegion) {
        final Matcher matcher = STREAM_ARN_PATTERN.matcher(input);
        if (matcher.matches()) {
            final String arnRegion = matcher.group("region");
            final Region region = (arnRegion != null) ? Region.of(arnRegion) : kinesisRegion;
            if ((kinesisRegion != null) && (region != kinesisRegion)) {
                throw new IllegalArgumentException(String.format(
                        "Cannot create StreamIdentifier for a region other than %s: %s", kinesisRegion, input));
            }
            return toStreamIdentifier(matcher, "", region);
        }
        return null;
    }

    private static StreamIdentifier toStreamIdentifier(final Matcher matcher, final String matchedEpoch,
            final Region kinesisRegion) {
        final String accountId = matcher.group("accountId");
        final String streamName = matcher.group("streamName");
        final Optional<Long> creationEpoch = matchedEpoch.isEmpty() ? Optional.empty()
                : Optional.of(Long.valueOf(matchedEpoch));
        final Optional<Arn> arn = StreamARNUtil.getStreamARN(streamName, kinesisRegion, accountId);

        if (!creationEpoch.isPresent() && !arn.isPresent()) {
            throw new IllegalArgumentException("Cannot create StreamIdentifier if missing both ARN and creation epoch");
        }

        return StreamIdentifier.builder()
                .accountIdOptional(Optional.of(accountId))
                .streamName(streamName)
                .streamCreationEpochOptional(creationEpoch)
                .streamARNOptional(arn)
                .build();
    }

}
