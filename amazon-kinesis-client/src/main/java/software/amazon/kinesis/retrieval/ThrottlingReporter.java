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
package software.amazon.kinesis.retrieval;

import org.slf4j.Logger;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class ThrottlingReporter {

    private final int maxConsecutiveWarnThrottles;
    private final String shardId;

    private int consecutiveThrottles = 0;

    public void throttled() {
        consecutiveThrottles++;
        String message = "Shard '" + shardId + "' has been throttled "
                + consecutiveThrottles + " consecutively";

        if (consecutiveThrottles > maxConsecutiveWarnThrottles) {
            getLog().error(message);
        } else {
            getLog().warn(message);
        }

    }

    public void success() {
        consecutiveThrottles = 0;
    }

    protected Logger getLog() {
        return log;
    }

}
