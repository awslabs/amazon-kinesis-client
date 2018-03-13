/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.multilang;

import java.io.BufferedReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Reads lines off the STDERR of the child process and prints them to this process's (the JVM's) STDERR and log.
 */
class DrainChildSTDERRTask extends LineReaderTask<Boolean> {

    private static final Log LOG = LogFactory.getLog(DrainChildSTDERRTask.class);

    DrainChildSTDERRTask() {
    }

    @Override
    protected HandleLineResult<Boolean> handleLine(String line) {
        LOG.error("Received error line from subprocess [" + line + "] for shard " + getShardId());
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
