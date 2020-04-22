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

package software.amazon.kinesis.processor;

import software.amazon.kinesis.common.StreamConfig;

import java.time.Duration;
import java.util.List;

/**
 * Interface for stream trackers. This is useful for KCL Workers that need
 * to consume data from multiple streams.
 * KCL will periodically probe this interface to learn about the new and old streams.
 */
public interface MultiStreamTracker {

    /**
     * Returns the list of stream config, to be processed by the current application.
     * Note that this method will be called periodically called by the KCL to learn about the new and old streams.
     *
     * @return List of StreamConfig
     */
    List<StreamConfig> streamConfigList();

    /**
     * Duration to wait before deleting the old streams in the lease table.
     * @return Wait time before deleting old streams
     */
    Duration waitPeriodToDeleteOldStreams();
}
