/*
 * Copyright 2026 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.leader;

import java.util.Collections;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.EntityType;

/**
 * Data model of the KCL Lock. This is used to elect a leader among
 * the worker which can then perform tasks that require to be done
 * once for the whole cluster: such as detecting new and deleted
 * streams in MultiStreamMode, running LeaseAssignmentManager etc.
 */
@Getter
@ToString(callSuper = true)
@Slf4j
@KinesisClientInternalApi
public class LeaderLock extends CoordinatorState {

    public static final String LEADER_HASH_KEY = "Leader";

    public LeaderLock() {
        super(LEADER_HASH_KEY, EntityType.CoordinatorStateType.LEADER_LOCK, Collections.emptyMap());
    }
}
