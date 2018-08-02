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
 * Thrown when requests are throttled by a service (e.g. DynamoDB when storing a checkpoint).
 */
public class ThrottlingException extends KinesisClientLibRetryableException {

    private static final long serialVersionUID = 1L;

    /**
     * @param message Message about what was throttled and any guidance we can provide.
     */
    public ThrottlingException(String message) {
        super(message);
    }

    /**
     * @param message provides more details about the cause and potential ways to debug/address.
     * @param e Underlying cause of the exception.
     */
    public ThrottlingException(String message, Exception e) {
        super(message, e);
    }

}
