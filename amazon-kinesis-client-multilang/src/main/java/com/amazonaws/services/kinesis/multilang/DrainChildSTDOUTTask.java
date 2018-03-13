/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.multilang;

import java.io.BufferedReader;

import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to drain the STDOUT of the child process. After the child process has been given a shutdown
 * message and responded indicating that it is shutdown, we attempt to close the input and outputs of that process so
 * that the process can exit.
 * 
 * To understand why this is necessary, consider the following scenario:
 * 
 * <ol>
 * <li>Child process responds that it is done with shutdown.</li>
 * <li>Child process prints debugging text to STDOUT that fills the pipe buffer so child becomes blocked.</li>
 * <li>Parent process doesn't drain child process's STDOUT.</li>
 * <li>Child process remains blocked.</li>
 * </ol>
 * 
 * To prevent the child process from becoming blocked in this way, it is the responsibility of the parent process to
 * drain the child process's STDOUT. We reprint each drained line to our log to permit debugging.
 */
@Slf4j
class DrainChildSTDOUTTask extends LineReaderTask<Boolean> {
    DrainChildSTDOUTTask() {
    }

    @Override
    protected HandleLineResult<Boolean> handleLine(String line) {
        log.info("Drained line for shard {}: {}", getShardId(), line);
        return new HandleLineResult<Boolean>();
    }

    @Override
    protected Boolean returnAfterException(Exception e) {
        log.info("Encountered exception while draining STDOUT of child process for shard {}", getShardId(), e);
        return false;
    }

    @Override
    protected Boolean returnAfterEndOfInput() {
        return true;
    }

    public LineReaderTask<Boolean> initialize(BufferedReader reader, String shardId) {
        return initialize(reader, shardId, "Draining STDOUT for " + shardId);
    }
}
