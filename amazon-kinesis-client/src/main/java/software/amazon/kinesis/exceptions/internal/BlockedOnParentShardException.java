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
 * Used internally in the Amazon Kinesis Client Library. Indicates that we cannot start processing data for a shard
 * because the data from the parent shard has not been completely processed (yet).
 */
public class BlockedOnParentShardException extends KinesisClientLibRetryableException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param message Error message.
     */
    public BlockedOnParentShardException(String message) {
        super(message);
    }

    /**
     * Constructor.
     * 
     * @param message Error message.
     * @param e Cause of the exception.
     */
    public BlockedOnParentShardException(String message, Exception e) {
        super(message, e);
    }

}
