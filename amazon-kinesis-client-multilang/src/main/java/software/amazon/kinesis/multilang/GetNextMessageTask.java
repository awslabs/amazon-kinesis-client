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
import java.io.IOException;

import software.amazon.kinesis.multilang.messages.Message;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Gets the next message off the STDOUT of the child process. Throws an exception if a message is not found before the
 * end of the input stream is reached.
 */
@Slf4j
class GetNextMessageTask extends LineReaderTask<Message> {
    private ObjectMapper objectMapper;

    private static final String EMPTY_LINE = "";

    /**
     * Constructor.
     * 
     * @param objectMapper An object mapper for decoding json messages from the input stream.
     */
    GetNextMessageTask(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Checks if a line is an empty line.
     * 
     * @param line A string
     * @return True if the line is an empty string, i.e. "", false otherwise.
     */
    static boolean isEmptyLine(String line) {
        return EMPTY_LINE.equals(line);
    }

    @Override
    protected HandleLineResult<Message> handleLine(String line) {
        try {
            /*
             * If the line is an empty line we don't bother logging anything because we expect the child process to be
             * nesting its messages between new lines, e.g. "\n<JSON message>\n". If there are no other entities writing
             * to the child process's STDOUT then this behavior will result in patterns like
             * "...\n<JSON message>\n\n<JSON message>\n..." which contains empty lines.
             */
            if (!isEmptyLine(line)) {
                return new HandleLineResult<Message>(objectMapper.readValue(line, Message.class));
            }
        } catch (IOException e) {
            log.info("Skipping unexpected line on STDOUT for shard {}: {}", getShardId(), line);
        }
        return new HandleLineResult<Message>();
    }

    @Override
    protected Message returnAfterException(Exception e) {
        throw new RuntimeException("Encountered an error while reading a line from STDIN for shard " + getShardId()
                + " so won't be able to return a message.", e);
    }

    @Override
    protected Message returnAfterEndOfInput() {
        throw new RuntimeException("Reached end of STDIN of child process for shard " + getShardId()
                + " so won't be able to return a message.");
    }

    public LineReaderTask<Message> initialize(BufferedReader reader, String shardId) {
        return initialize(reader, shardId, "Reading next message from STDIN for " + shardId);
    }
}
