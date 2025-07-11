/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.coordinator.assignment.exclusion;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.coordinator.CoordinatorState;

@Getter
@RequiredArgsConstructor
public class WorkerIdExclusionState extends CoordinatorState {

    public static final String WORKER_ID_EXCLUSION_HASH_KEY = "WorkerIdExclusion";
    public static final String REGEX_HASH_KEY = "r";
    public static final String EXPIRATION_HASH_KEY = "e";

    private final Pattern regex;
    private final Instant expirationInstant;

    public static WorkerIdExclusionState fromDynamoRecord(Map<String, AttributeValue> dynamoRecord) {
        return new WorkerIdExclusionState(getRegexAttribute(dynamoRecord), getExpirationInstantAttribute(dynamoRecord));
    }

    public static Pattern getRegexAttribute(Map<String, AttributeValue> dynamoRecord) {
        return Pattern.compile(dynamoRecord.get(REGEX_HASH_KEY).s());
    }

    public static Instant getExpirationInstantAttribute(Map<String, AttributeValue> dynamoRecord) {
        return Instant.ofEpochMilli(
                Long.valueOf(dynamoRecord.get(EXPIRATION_HASH_KEY).n()));
    }

    @Override
    public boolean equals(Object other) {
        if (this == null ^ other == null) {
            return false;
        } else if (!(other instanceof WorkerIdExclusionState)) {
            return false;
        } else {
            WorkerIdExclusionState o = (WorkerIdExclusionState) other;
            return sameRegex(o) && this.expirationInstant == o.getExpirationInstant();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.regex, this.expirationInstant);
    }

    public boolean sameRegex(@NonNull WorkerIdExclusionState other) {
        Pattern tr = this.regex;
        Pattern or = other.getRegex();

        boolean trn = tr == null;
        boolean orn = or == null;

        if (trn || orn) {
            return trn && orn;
        }
        return tr.pattern().equals(or.pattern());
    }
}
