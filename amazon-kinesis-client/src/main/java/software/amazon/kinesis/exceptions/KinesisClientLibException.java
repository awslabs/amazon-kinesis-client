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
 * Abstract class for exceptions of the Amazon Kinesis Client Library.
 * This exception has two subclasses:
 * 1. KinesisClientLibNonRetryableException
 * 2. KinesisClientLibRetryableException.
 */
public abstract class KinesisClientLibException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param message Message of with details of the exception.
     */
    public KinesisClientLibException(String message) {
        super(message);
    }

    /**
     * Constructor.
     * 
     * @param message Message with details of the exception.
     * @param cause Cause.
     */
    public KinesisClientLibException(String message, Throwable cause) {
        super(message, cause);
    }

}
