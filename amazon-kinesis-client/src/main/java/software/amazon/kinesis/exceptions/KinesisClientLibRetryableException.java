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
 * Retryable exceptions (e.g. transient errors). The request/operation is expected to succeed upon (back off and) retry.
 */
public abstract class KinesisClientLibRetryableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param message Message with details about the exception.
     */
    public KinesisClientLibRetryableException(String message) {
        super(message);
    }

    /**
     * Constructor.
     * 
     * @param message Message with details about the exception.
     * @param e Cause.
     */
    public KinesisClientLibRetryableException(String message, Exception e) {
        super(message, e);
    }
}
