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
package software.amazon.kinesis.lifecycle;

/**
 * Enumerates types of outcome of tasks executed as part of processing a shard.
 */
public enum TaskOutcome {
    /**
     * Denotes a successful task outcome.
     */
    SUCCESSFUL,
    /**
     * Denotes that the last record from the shard has been read/consumed.
     */
    END_OF_SHARD,
    /**
     * Denotes a failure or exception during processing of the shard.
     */
    FAILURE
}
