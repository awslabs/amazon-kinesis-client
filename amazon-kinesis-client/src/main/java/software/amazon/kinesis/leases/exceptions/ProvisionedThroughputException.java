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
 * Indicates that a lease operation has failed due to lack of provisioned throughput for a DynamoDB table.
 */
public class ProvisionedThroughputException extends LeasingException {

    private static final long serialVersionUID = 1L;

    public ProvisionedThroughputException(Throwable e) {
        super(e);
    }

    public ProvisionedThroughputException(String message, Throwable e) {
        super(message, e);
    }

}
