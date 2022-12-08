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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.utils.Validate;

import java.util.Optional;
import java.util.regex.Pattern;

import static software.amazon.kinesis.common.StreamARNUtil.getOptionalStreamARN;
import static software.amazon.kinesis.common.StreamARNUtil.getOptionalStreamARNFromDescribeStreamSummary;
import static software.amazon.kinesis.common.StreamARNUtil.getStreamName;

@Slf4j
@EqualsAndHashCode
@Getter
@Accessors(fluent = true)
public class StreamIdentifier {
    private final Optional<String> accountIdOptional;
    private final String streamName;
    @Setter
    private Optional<String> streamARN;
    private final Optional<Long> streamCreationEpochOptional;

    private static final String DELIMITER = ":";
    private static final Pattern PATTERN = Pattern.compile(".*" + ":" + ".*" + ":" + "[0-9]*");

    private StreamIdentifier(@NonNull String accountId, @NonNull String streamNameOrARN, @NonNull Long streamCreationEpoch) {
        this.accountIdOptional = Optional.of(accountId);
        this.streamName = getStreamName(streamNameOrARN);
        this.streamARN = getOptionalStreamARN(streamNameOrARN);
        this.streamCreationEpochOptional = Optional.of(streamCreationEpoch);
    }

    private StreamIdentifier(@NonNull String streamNameOrARN) {
        this.accountIdOptional = Optional.empty();
        this.streamName = getStreamName(streamNameOrARN);
        this.streamARN = getOptionalStreamARN(streamNameOrARN);
        this.streamCreationEpochOptional = Optional.empty();
    }

    /**
     * Serialize the current StreamIdentifier instance.
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
     * The serialized stream identifier should be of the format account:stream:creationepoch
     * @param streamIdentifierSer
     * @return StreamIdentifier
     */
    public static StreamIdentifier multiStreamInstance(String streamIdentifierSer) {
        if (PATTERN.matcher(streamIdentifierSer).matches()) {
            final String[] split = streamIdentifierSer.split(DELIMITER);
            return new StreamIdentifier(split[0], split[1], Long.parseLong(split[2]));
        } else {
            throw new IllegalArgumentException("Unable to deserialize StreamIdentifier from " + streamIdentifierSer);
        }
    }

    /**
     * Create a multi stream instance for StreamIdentifier from accountId, streamNameOrARN and creationEpoch
     * The serialized stream identifier should be of the format account:stream:creationepoch
     * @param accountId The account's 12-digit ID, which is hosting the stream
     * @param  streamNameOrARN Although streamName and streamARN can both be supplied, streamARN is preferred
     * @return creationEpoch The stream's creation time stamp
     */
    public static StreamIdentifier multiStreamInstance(String accountId, String streamNameOrARN, Long creationEpoch) {
        return new StreamIdentifier(accountId, streamNameOrARN, creationEpoch);
    }

    /**
     * Create a single stream instance for StreamIdentifier from stream name or stream arn.
     * @param streamNameOrARN
     * @return StreamIdentifier
     */
    public static StreamIdentifier singleStreamInstance(String streamNameOrARN) {
        Validate.notEmpty(streamNameOrARN, "StreamName should not be empty");
        return new StreamIdentifier(streamNameOrARN);
    }
}
