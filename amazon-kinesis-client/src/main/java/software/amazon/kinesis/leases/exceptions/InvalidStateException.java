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
package software.amazon.kinesis.leases.exceptions;

/**
 * Indicates that a lease operation has failed because DynamoDB is an invalid state. The most common example is failing
 * to create the DynamoDB table before doing any lease operations.
 */
public class InvalidStateException extends LeasingException {

    private static final long serialVersionUID = 1L;

    public InvalidStateException(Throwable e) {
        super(e);
    }

    public InvalidStateException(String message, Throwable e) {
        super(message, e);
    }

    public InvalidStateException(String message) {
        super(message);
    }

}
