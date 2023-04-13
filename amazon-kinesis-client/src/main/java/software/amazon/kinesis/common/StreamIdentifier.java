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

import com.google.common.base.Joiner;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.Validate;

import java.util.Optional;
import java.util.regex.Pattern;

@Builder(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
public class StreamIdentifier {
    @Builder.Default
    private final Optional<String> accountIdOptional = Optional.empty();
    private final String streamName;
    @Builder.Default
    private final Optional<Long> streamCreationEpochOptional = Optional.empty();
    @Builder.Default
    private final Optional<Arn> streamARNOptional = Optional.empty();

    private static final String DELIMITER = ":";
    private static final Pattern PATTERN = Pattern.compile(".*" + ":" + ".*" + ":" + "[0-9]*" + ":?([a-z]{2}(-gov)?-[a-z]+-\\d{1})?");

    /**
     * Serialize the current StreamIdentifier instance.
     * TODO: Consider appending region info for cross-account consumer support
     * @return
     */
    public String serialize() {
        return accountIdOptional.isPresent() ?
                Joiner.on(DELIMITER).join(accountIdOptional.get(), streamName, streamCreationEpochOptional.get()) :
                streamName;
    }

    @Override
    public String toString() {
        return serialize();
    }

    /**
     * Create a multi stream instance for StreamIdentifier from serialized stream identifier.
     * See the format of a serialized stream identifier at {@link StreamIdentifier#multiStreamInstance(String, Region)}
     * @param streamIdentifierSer
     * @return StreamIdentifier
     */
    public static StreamIdentifier multiStreamInstance(String streamIdentifierSer) {
        return multiStreamInstance(streamIdentifierSer, null);
    }

    /**
     * Create a multi stream instance for StreamIdentifier from serialized stream identifier.
     * @param streamIdentifierSer The serialized stream identifier should be of the format
     *                            account:stream:creationepoch[:region]
     * @param kinesisRegion This nullable region is used to construct the optional StreamARN
     * @return StreamIdentifier
     */
    public static StreamIdentifier multiStreamInstance(String streamIdentifierSer, Region kinesisRegion) {
        if (PATTERN.matcher(streamIdentifierSer).matches()) {
            final String[] split = streamIdentifierSer.split(DELIMITER);
            final String streamName = split[1];
            final Optional<String> accountId = Optional.ofNullable(split[0]);
            StreamIdentifierBuilder builder = StreamIdentifier.builder()
                    .accountIdOptional(accountId)
                    .streamName(streamName)
                    .streamCreationEpochOptional(Optional.of(Long.parseLong(split[2])));
            final Region region = (split.length == 4) ?
                    Region.of(split[3]) :   // Use the region extracted from the serialized string, which matches the regex pattern
                    kinesisRegion;          // Otherwise just use the provided region
            final Optional<Arn> streamARN = StreamARNUtil.getStreamARN(streamName, region, accountId);
            return builder.streamARNOptional(streamARN).build();
        } else {
            throw new IllegalArgumentException("Unable to deserialize StreamIdentifier from " + streamIdentifierSer);
        }
    }

    /**
     * Create a single stream instance for StreamIdentifier from stream name.
     * @param streamName
     * @return StreamIdentifier
     */
    public static StreamIdentifier singleStreamInstance(String streamName) {
        return singleStreamInstance(streamName, null);
    }

    /**
     * Create a single stream instance for StreamIdentifier from the provided stream name and kinesisRegion.
     * This method also constructs the optional StreamARN based on the region info.
     * @param streamName
     * @param kinesisRegion
     * @return StreamIdentifier
     */
    public static StreamIdentifier singleStreamInstance(String streamName, Region kinesisRegion) {
        Validate.notEmpty(streamName, "StreamName should not be empty");
        return StreamIdentifier.builder()
                .streamName(streamName)
                .streamARNOptional(StreamARNUtil.getStreamARN(streamName, kinesisRegion))
                .build();
    }
}
