/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
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

import java.util.Date;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Class that houses the entities needed to specify the position in the stream from where a new application should
 * start.
 */
@ToString
@EqualsAndHashCode
public class InitialPositionInStreamExtended {

    private final InitialPositionInStream position;
    private final Date timestamp;

    /**
     * This is scoped as private to forbid callers from using it directly and to convey the intent to use the
     * static methods instead.
     *
     * @param position  One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. The Amazon Kinesis Client Library will start
     *                  fetching records from this position when the application starts up if there are no checkpoints.
     *                  If there are checkpoints, we will process records from the checkpoint position.
     * @param timestamp The timestamp to use with the AT_TIMESTAMP value for initialPositionInStream.
     */
    private InitialPositionInStreamExtended(final InitialPositionInStream position, final Date timestamp) {
        this.position = position;
        this.timestamp = timestamp;
    }

    /**
     * Get the initial position in the stream where the application should start from.
     *
     * @return The initial position in stream.
     */
    public InitialPositionInStream getInitialPositionInStream() {
        return this.position;
    }

    /**
     * Get the timestamp from where we need to start the application.
     * Valid only for initial position of type AT_TIMESTAMP, returns null for other positions.
     *
     * @return The timestamp from where we need to start the application.
     */
    public Date getTimestamp() {
        return this.timestamp;
    }

    public static InitialPositionInStreamExtended newInitialPosition(final InitialPositionInStream position) {
        switch (position) {
            case LATEST:
                return new InitialPositionInStreamExtended(InitialPositionInStream.LATEST, null);
            case TRIM_HORIZON:
                return new InitialPositionInStreamExtended(InitialPositionInStream.TRIM_HORIZON, null);
            default:
                throw new IllegalArgumentException("Invalid InitialPosition: " + position);
        }
    }

    public static InitialPositionInStreamExtended newInitialPositionAtTimestamp(final Date timestamp) {
        if (timestamp == null) {
            throw new IllegalArgumentException("Timestamp must be specified for InitialPosition AT_TIMESTAMP");
        }
        return new InitialPositionInStreamExtended(InitialPositionInStream.AT_TIMESTAMP, timestamp);
    }
}
