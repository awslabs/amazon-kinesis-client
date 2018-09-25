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

/**
 * Callback interface for interacting with the DynamoDB lease table post creation.
 */
@FunctionalInterface
public interface TableCreatorCallback {
    /**
     * NoOp implemetation for TableCreatorCallback
     */
    TableCreatorCallback NOOP_TABLE_CREATOR_CALLBACK = (TableCreatorCallbackInput tableCreatorCallbackInput) -> {
        // Do nothing
    };

    /**
     * Actions needed to be performed on the DynamoDB lease table once the table has been created and is in the ACTIVE
     * status. Will not be called if the table previously exists.
     *
     * @param tableCreatorCallbackInput
     *            Input object for table creator
     */
    void performAction(TableCreatorCallbackInput tableCreatorCallbackInput);

}
