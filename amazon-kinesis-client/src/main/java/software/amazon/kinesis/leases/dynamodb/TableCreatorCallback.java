/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.kinesis.leases.dynamodb;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

/**
 * Callback interface for interacting with the DynamoDB lease table post creation.
 */
public interface TableCreatorCallback {

    /**
     * Actions needed to be performed on the DynamoDB lease table once the table has been created and is in the ACTIVE
     * status. Will not be called if the table previously exists.
     *
     * @param client
     *            DynamoDB client used to interact with the DynamoDB service
     * @param tableName
     *            Table name of the lease table
     */
    void performAction(DynamoDbAsyncClient client, String tableName);

}
