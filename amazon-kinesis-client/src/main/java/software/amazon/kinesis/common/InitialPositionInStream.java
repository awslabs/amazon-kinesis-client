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

/**
 * Used to specify the position in the stream where a new application should start from.
 * This is used during initial application bootstrap (when a checkpoint doesn't exist for a shard or its parents).
 */
public enum InitialPositionInStream {
    /**
     * Start after the most recent data record (fetch new data).
     */
    LATEST,

    /**
     * Start from the oldest available data record.
     */
    TRIM_HORIZON,

    /**
     * Start from the record at or after the specified server-side timestamp.
     */
    AT_TIMESTAMP
}
