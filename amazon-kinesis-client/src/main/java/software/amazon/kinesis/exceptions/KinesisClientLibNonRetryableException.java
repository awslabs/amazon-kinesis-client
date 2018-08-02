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
