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
