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
package software.amazon.kinesis.exceptions.internal;

import software.amazon.kinesis.exceptions.KinesisClientLibRetryableException;

/**
 * Thrown when we encounter issues when reading/writing information (e.g. shard information from Kinesis may not be
 * current/complete).
 */
public class KinesisClientLibIOException extends KinesisClientLibRetryableException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param message Error message.
     */
    public KinesisClientLibIOException(String message) {
        super(message);
    }

    /**
     * Constructor.
     * 
     * @param message Error message.
     * @param e Cause.
     */
    public KinesisClientLibIOException(String message, Exception e) {
        super(message, e);
    }
}
