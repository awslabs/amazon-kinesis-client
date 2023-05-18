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
import software.amazon.awssdk.utils.Validate;

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
    private final Optional<Long> streamCreationEpochOptional = Optional.empty();
    @Builder.Default
    @EqualsAndHashCode.Exclude
    private final Optional<Arn> streamArnOptional = Optional.empty();

    /**
     * Pattern for a serialized {@link StreamIdentifier}. The valid format is
     * {@code <accountId>:<streamName>:<creationEpoch>}.
     */
    private static final Pattern STREAM_IDENTIFIER_PATTERN = Pattern.compile(
            "(?<accountId>[0-9]+):(?<streamName>[^:]+):(?<creationEpoch>[0-9]+)");

    /**
     * Pattern for a stream Arn. The valid format is
     * {@code arn:aws:kinesis:<region>:<accountId>:stream:<streamName>}
     * where {@code region} is the id representation of a {@link Region}.
     */
    private static final Pattern STREAM_ARN_PATTERN = Pattern.compile(
            "arn:aws[^:]*:kinesis:(?<region>[-a-z0-9]+):(?<accountId>[0-9]{12}):stream/(?<streamName>.+)");

    /**
     * Serialize the current StreamIdentifier instance.
     *
     * @return a String of {@code account:stream:creationEpoch} in multi-stream mode
     *         or {@link #streamName} in single-stream mode.
     */
    public String serialize() {
        if (!streamCreationEpochOptional.isPresent()) {
            // creation epoch is expected to be empty in single-stream mode
            return streamName;
        }

        final char delimiter = ':';
        final StringBuilder sb = new StringBuilder()
                .append(accountIdOptional.get()).append(delimiter)
                .append(streamName).append(delimiter)
                .append(streamCreationEpochOptional.get());
        return sb.toString();
    }

    @Override
    public String toString() {
        return serialize();
    }

    /**
     * Create a multi stream instance for StreamIdentifier from serialized stream identifier
     * of format {@link #STREAM_IDENTIFIER_PATTERN}
     *
     * @param streamIdentifierSer a String of {@code account:stream:creationEpoch}
     * @return StreamIdentifier with {@link #accountIdOptional} and {@link #streamCreationEpochOptional} present
     */
    public static StreamIdentifier multiStreamInstance(String streamIdentifierSer) {
        final Matcher matcher = STREAM_IDENTIFIER_PATTERN.matcher(streamIdentifierSer);
        if (matcher.matches()) {
            final String accountId = matcher.group("accountId");
            final String streamName = matcher.group("streamName");
            final Long creationEpoch = Long.valueOf(matcher.group("creationEpoch"));

            validateCreationEpoch(creationEpoch);

            return StreamIdentifier.builder()
                    .accountIdOptional(Optional.of(accountId))
                    .streamName(streamName)
                    .streamCreationEpochOptional(Optional.of(creationEpoch))
                    .build();
        }

        throw new IllegalArgumentException("Unable to deserialize StreamIdentifier from " + streamIdentifierSer);
    }

    /**
     * Create a multi stream instance for StreamIdentifier from stream {@link Arn}.
     *
     * @param streamArn an {@link Arn} of format {@link #STREAM_ARN_PATTERN}
     * @param creationEpoch creation epoch of the stream
     *                      an incorrect creationEpoch may result in erroneous/deleterious behavior in lease management,
     *                      such as when an epoch is reused for two editions of the same stream
     *                      (e.g., create stream A at epoch t1, create StreamIdentifer(A, t1), delete A,
     *                      create stream A at epoch t2, reuse old epoch to create StreamIdentifier(A, t1))
     * @return StreamIdentifier with {@link #accountIdOptional}, {@link #streamCreationEpochOptional},
     *         and {@link #streamArnOptional} present
     */
    public static StreamIdentifier multiStreamInstance(Arn streamArn, long creationEpoch) {
        validateArn(streamArn);
        validateCreationEpoch(creationEpoch);

        return StreamIdentifier.builder()
                .accountIdOptional(streamArn.accountId())
                .streamName(streamArn.resource().resource())
                .streamCreationEpochOptional(Optional.of(creationEpoch))
                .streamArnOptional(Optional.of(streamArn))
                .build();
    }

    /**
     * Create a single stream instance for StreamIdentifier from stream name.
     *
     * @param streamName stream name of a Kinesis stream
     */
    public static StreamIdentifier singleStreamInstance(String streamName) {
        Validate.notEmpty(streamName, "StreamName should not be empty");

        return StreamIdentifier.builder()
                .streamName(streamName)
                .build();
    }

    /**
     * Create a single stream instance for StreamIdentifier from AWS Kinesis stream {@link Arn}.
     *
     * @param streamArn AWS Arn of a Kinesis stream
     * @return StreamIdentifier with {@link #accountIdOptional} and {@link #streamArnOptional} present
     */
    public static StreamIdentifier singleStreamInstance(Arn streamArn) {
        validateArn(streamArn);

        return StreamIdentifier.builder()
                .accountIdOptional(streamArn.accountId())
                .streamName(streamArn.resource().resource())
                .streamArnOptional(Optional.of(streamArn))
                .build();
    }

    private static void validateArn(Arn streamArn) {
        if (!STREAM_ARN_PATTERN.matcher(streamArn.toString()).matches() || !streamArn.region().isPresent()) {
            throw new IllegalArgumentException("Unable to create a StreamIdentifier from " + streamArn);
        }
    }

    private static void validateCreationEpoch(long creationEpoch) {
        if (creationEpoch <= 0) {
            throw new IllegalArgumentException(
                    "Unable to create a StreamIdentifier from invalid creationEpoch " + creationEpoch);
        }
    }

}
