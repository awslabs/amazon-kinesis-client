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

package software.amazon.kinesis.utils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.DdbTableConfig;

import static java.util.Objects.nonNull;

@Slf4j
@KinesisClientInternalApi
public final class DdbUtil {

    @NonNull
    public static Supplier<CompletableFuture<CreateTableResponse>> tableCreator(
            final Supplier<List<KeySchemaElement>> keySchemaProvider,
            final Supplier<List<AttributeDefinition>> attributeDefinitionProvider,
            final DdbTableConfig tableConfig,
            final DynamoDbAsyncClient dynamoDbAsyncClient) {
        final CreateTableRequest.Builder createTableRequest = CreateTableRequest.builder()
                .tableName(tableConfig.tableName())
                .keySchema(keySchemaProvider.get())
                .attributeDefinitions(attributeDefinitionProvider.get())
                .deletionProtectionEnabled(tableConfig.deletionProtectionEnabled());

        if (nonNull(tableConfig.tags()) && !tableConfig.tags().isEmpty()) {
            createTableRequest.tags(tableConfig.tags());
        }

        if (tableConfig.billingMode() == BillingMode.PROVISIONED) {
            log.info(
                    "Creating table {} in provisioned mode with {}wcu and {}rcu",
                    tableConfig.tableName(),
                    tableConfig.writeCapacity(),
                    tableConfig.readCapacity());
            createTableRequest.provisionedThroughput(ProvisionedThroughput.builder()
                    .readCapacityUnits(tableConfig.readCapacity())
                    .writeCapacityUnits(tableConfig.writeCapacity())
                    .build());
        }
        createTableRequest.billingMode(tableConfig.billingMode());
        return () -> dynamoDbAsyncClient.createTable(createTableRequest.build());
    }

    public static CompletableFuture<UpdateContinuousBackupsResponse> pitrEnabler(
            final DdbTableConfig tableConfig, final DynamoDbAsyncClient dynamoDbAsyncClient) {
        if (tableConfig.pointInTimeRecoveryEnabled()) {
            final UpdateContinuousBackupsRequest request = UpdateContinuousBackupsRequest.builder()
                    .tableName(tableConfig.tableName())
                    .pointInTimeRecoverySpecification(builder -> builder.pointInTimeRecoveryEnabled(true))
                    .build();
            return dynamoDbAsyncClient.updateContinuousBackups(request);
        }
        return CompletableFuture.completedFuture(null);
    }
}
