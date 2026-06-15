/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.coordinator.delegate;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;

@KinesisClientInternalApi
public class LeaseTableCoordinatorStateDAODelegate extends CoordinatorStateDAODelegate {

    public LeaseTableCoordinatorStateDAODelegate(final DynamoDbAsyncClient dynamoDbAsyncClient, String leaseTableName) {
        super(dynamoDbAsyncClient, leaseTableName, DynamoDBLeaseSerializer.LEASE_KEY_KEY);
    }

    @Override
    public void initialize() {
        // No-op for lease table — always available
    }
}
