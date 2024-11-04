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
package software.amazon.kinesis.coordinator;

import java.util.Map;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * DataModel for CoordinatorState, this data model is used to store various state information required
 * for coordination across the KCL worker fleet. Therefore, the model follows a flexible schema.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
@KinesisClientInternalApi
public class CoordinatorState {
    public static final String COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME = "key";

    /**
     * Key value for the item in the CoordinatorState table used for leader
     * election among the KCL workers. The attributes relevant to this item
     * is dictated by the DDB Lock client implementation that is used to
     * provide mutual exclusion.
     */
    public static final String LEADER_HASH_KEY = "Leader";

    private String key;

    private Map<String, AttributeValue> attributes;
}
