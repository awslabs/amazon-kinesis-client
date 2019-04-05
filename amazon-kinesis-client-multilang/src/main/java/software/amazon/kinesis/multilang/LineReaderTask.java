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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;

import lombok.extern.slf4j.Slf4j;

/**
 * This abstract class captures the process of reading from an input stream. Three methods must be provided for
 * implementations to work.
 * <ol>
 * <li> {@link #handleLine(String)}</li>
 * <li> {@link #returnAfterEndOfInput()}</li>
 * <li> {@link #returnAfterException(Exception)}</li>
 * </ol>
 * 
 * @param <T>
 */
@Slf4j
abstract class LineReaderTask<T> implements Callable<T> {
    private BufferedReader reader;

    private String description;

    private String shardId;

    LineReaderTask() {
    }

    /**
     * Reads lines off the input stream until a return value is set, or an exception is encountered, or the end of the
     * input stream is reached. Will call the appropriate methods in each case. This is the shared piece of logic
     * between any tasks that need to read from a child process's STDOUT or STDERR.
     */
    @Override
    public T call() throws Exception {
        String nextLine = null;
        try {
            log.info("Starting: {}", description);
            while ((nextLine = reader.readLine()) != null) {
                HandleLineResult<T> result = handleLine(nextLine);
                if (result.hasReturnValue()) {
                    return result.returnValue();
                }
            }
        } catch (IOException e) {
            return returnAfterException(e);
        }
        log.info("Stopping: {}", description);
        return returnAfterEndOfInput();
    }

    /**
     * Handle a line read from the input stream. The return value indicates whether the enclosing read-loop should
     * return from the {@link #call()} function by having a value, indicating that value should be returned immediately
     * without reading further, or not having a value, indicating that more lines of input need to be read before
     * returning.
     * 
     * @param line A line read from the input stream.
     * @return HandleLineResult<T> which may or may not have a has return value, indicating to return or not return yet
     *         respectively.
     */
    protected abstract HandleLineResult<T> handleLine(String line);

    /**
     * This method will be called if there is an error while reading from the input stream. The return value of this
     * method will be returned as the result of this Callable unless an Exception is thrown. If an Exception is thrown
     * then that exception will be thrown by the Callable.
     * 
     * @param e An exception that occurred while reading from the input stream.
     * @return What to return.
     */
    protected abstract T returnAfterException(Exception e) throws Exception;

    /**
     * This method will be called once the end of the input stream is reached. The return value of this method will be
     * returned as the result of this Callable. Implementations of this method are welcome to throw a runtime exception
     * to indicate that the task was unsuccessful.
     * 
     * @return What to return.
     */
    protected abstract T returnAfterEndOfInput();

    /**
     * Allows subclasses to provide more detailed logs. Specifically, this allows the drain tasks and GetNextMessageTask
     * to log which shard they're working on.
     * 
     * @return The shard id
     */
    public String getShardId() {
        return this.shardId;
    }

    /**
     * The description should be a string explaining what this particular LineReader class does.
     * 
     * @return The description.
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * The result of a call to {@link LineReaderTask#handleLine(String)}. Allows implementations of that method to
     * indicate whether a particular invocation of that method produced a return for this task or not. If a return value
     * doesn't exist the {@link #call()} method will continue to the next line.
     * 
     * @param <V>
     */
    protected class HandleLineResult<V> {

        private boolean hasReturnValue;
        private V returnValue;

        HandleLineResult() {
            this.hasReturnValue = false;
        }

        HandleLineResult(V returnValue) {
            this.hasReturnValue = true;
            this.returnValue = returnValue;
        }

        boolean hasReturnValue() {
            return this.hasReturnValue;
        }

        V returnValue() {
            if (hasReturnValue()) {
                return this.returnValue;
            } else {
                throw new RuntimeException("There was no value to return.");
            }
        }
    }

    /**
     * An initialization method allows us to delay setting the attributes of this class. Some of the attributes, stream
     * and shardId, are not known to the {@link MultiLangRecordProcessorFactory} when it constructs a
     * {@link MultiLangShardRecordProcessor} but are later determined when
     * {@link MultiLangShardRecordProcessor#initialize(String)} is called. So we follow a pattern where the attributes are
     * set inside this method instead of the constructor so that this object will be initialized when all its attributes
     * are known to the record processor.
     * 
     * @param stream
     * @param shardId
     * @param description
     * @return
     */
    protected LineReaderTask<T> initialize(InputStream stream, String shardId, String description) {
        return this.initialize(new BufferedReader(new InputStreamReader(stream)), shardId, description);
    }

    /**
     * @param reader
     * @param shardId
     * @param description
     * @return
     */
    protected LineReaderTask<T> initialize(BufferedReader reader, String shardId, String description) {
        this.reader = reader;
        this.shardId = shardId;
        this.description = description;
        return this;
    }

}
