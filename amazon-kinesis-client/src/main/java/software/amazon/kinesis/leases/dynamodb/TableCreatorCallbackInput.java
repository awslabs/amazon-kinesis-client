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

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

/**
 *
 */
@Builder(toBuilder = true)
@Data
@Accessors(fluent = true)
@ToString
@EqualsAndHashCode
public class TableCreatorCallbackInput {
    @NonNull
    private final DynamoDbAsyncClient dynamoDbClient;
    @NonNull
    private final String tableName;
}
