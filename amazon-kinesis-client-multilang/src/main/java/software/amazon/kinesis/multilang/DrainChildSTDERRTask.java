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
package software.amazon.kinesis.multilang;

import java.io.BufferedReader;

import lombok.extern.slf4j.Slf4j;

/**
 * Reads lines off the STDERR of the child process and prints them to this process's (the JVM's) STDERR and log.
 */
@Slf4j
class DrainChildSTDERRTask extends LineReaderTask<Boolean> {
    DrainChildSTDERRTask() {
    }

    @Override
    protected HandleLineResult<Boolean> handleLine(String line) {
        log.error("Received error line from subprocess [{}] for shard {}", line, getShardId());
        System.err.println(line);
        return new HandleLineResult<Boolean>();
    }

    @Override
    protected Boolean returnAfterException(Exception e) {
        return false;
    }

    @Override
    protected Boolean returnAfterEndOfInput() {
        return true;
    }

    public LineReaderTask<Boolean> initialize(BufferedReader reader, String shardId) {
        return initialize(reader, shardId, "Draining STDERR for " + shardId);
    }
}
