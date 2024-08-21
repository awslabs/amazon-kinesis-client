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

import java.util.Optional;

import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class RequestDetails {

    /**
     * Placeholder for logging when no successful request has been made.
     */
    private static final String NONE = "NONE";

    private final Optional<String> requestId;
    private final Optional<String> timestamp;

    public RequestDetails() {
        this.requestId = Optional.empty();
        this.timestamp = Optional.empty();
    }

    public RequestDetails(String requestId, String timestamp) {
        this.requestId = Optional.of(requestId);
        this.timestamp = Optional.of(timestamp);
    }

    /**
     * Gets last successful request's request id.
     *
     * @return requestId associated with last successful request.
     */
    public String getRequestId() {
        return requestId.orElse(NONE);
    }

    /**
     * Gets last successful request's timestamp.
     *
     * @return timestamp associated with last successful request.
     */
    public String getTimestamp() {
        return timestamp.orElse(NONE);
    }

    @Override
    public String toString() {
        return String.format("request id - %s, timestamp - %s", getRequestId(), getTimestamp());
    }
}
