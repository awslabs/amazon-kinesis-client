/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.leases.dynamodb;

/**
 * Callback interface for interacting with the DynamoDB lease table post creation.
 */
@FunctionalInterface
public interface TableCreatorCallback {
    /**
     * NoOp implementation for TableCreatorCallback
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
