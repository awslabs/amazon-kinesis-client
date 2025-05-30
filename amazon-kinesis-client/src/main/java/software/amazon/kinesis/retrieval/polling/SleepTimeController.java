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

package software.amazon.kinesis.retrieval.polling;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;

@KinesisClientInternalApi
public interface SleepTimeController {

    /**
     * Calculates the sleep time in milliseconds before the next GetRecords call.
     *
     * @param sleepTimeControllerConfig contains the last successful call time and the idle time between calls.
     * @return the sleep time in milliseconds.
     */
    long getSleepTimeMillis(SleepTimeControllerConfig sleepTimeControllerConfig);
}
