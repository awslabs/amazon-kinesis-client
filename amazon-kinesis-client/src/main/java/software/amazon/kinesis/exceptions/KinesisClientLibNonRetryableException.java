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
package software.amazon.kinesis.exceptions;

/**
 * Non-retryable exceptions. Simply retrying the same request/operation is not expected to succeed.
 * 
 */
public abstract class KinesisClientLibNonRetryableException extends KinesisClientLibException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param message Message.
     */
    public KinesisClientLibNonRetryableException(String message) {
        super(message);
    }

    /**
     * Constructor.
     * 
     * @param message Message.
     * @param e Cause.
     */
    public KinesisClientLibNonRetryableException(String message, Exception e) {
        super(message, e);
    }
}
