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

package software.amazon.kinesis.leases.dynamodb;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.leases.TableCreatorCallback;

/**
 * This implementation of the TableCreatorCallback does nothing.
 */
public class NoOpTableCreatorCallback implements TableCreatorCallback {
    /**
     * {@inheritDoc}
     */
    @Override
    public void performAction(final DynamoDbAsyncClient client, final String tableName) {
        // Does nothing
    }
}
