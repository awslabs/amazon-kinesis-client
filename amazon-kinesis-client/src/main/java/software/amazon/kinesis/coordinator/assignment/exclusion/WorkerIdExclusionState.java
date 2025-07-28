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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.coordinator.CoordinatorState;

@Getter
public class WorkerIdExclusionState extends CoordinatorState {

    public static final String WORKER_ID_EXCLUSION_HASH_KEY = "WorkerIdExclusion";
    public static final String REGEX_HASH_KEY = "r";
    public static final String EXPIRATION_HASH_KEY = "e";
    public static final String ONLY_EXCLUDING_LEADERSHIP_HASH_KEY = "l";

    private static final String DEFAULT_REGEX = "(?!)";
    private static final Long DEFAULT_EXPIRATION = Long.MAX_VALUE;
    private static final boolean DEFAULT_EXCLUDING_LEADERSHIP = false;

    private final Pattern regex;
    private final Instant expirationInstant;
    private final boolean onlyExcludingLeadership;

    public WorkerIdExclusionState() {
        this(DEFAULT_REGEX, DEFAULT_EXPIRATION, DEFAULT_EXCLUDING_LEADERSHIP);
    }

    public WorkerIdExclusionState(String regex, long expirationInstant, boolean onlyExcludingLeadership) {
        this.initializeParentFields();
        this.putAttributeValues(regex, expirationInstant, onlyExcludingLeadership);

        this.regex = Pattern.compile(regex);
        this.expirationInstant = Instant.ofEpochMilli(expirationInstant);
        this.onlyExcludingLeadership = onlyExcludingLeadership;
    }

    private void initializeParentFields() {
        this.setKey(WORKER_ID_EXCLUSION_HASH_KEY);
        this.setAttributes(new HashMap<String, AttributeValue>());
    }

    private void putAttributeValues(String regex, long expirationInstant, boolean onlyExcludingLeadership) {
        Map<String, AttributeValue> attributes = this.getAttributes();

        attributes.put(REGEX_HASH_KEY, AttributeValue.fromS(regex));
        attributes.put(EXPIRATION_HASH_KEY, AttributeValue.fromN(String.valueOf(expirationInstant)));
        attributes.put(ONLY_EXCLUDING_LEADERSHIP_HASH_KEY, AttributeValue.fromBool(onlyExcludingLeadership));
    }

    public static WorkerIdExclusionState fromDynamoRecord(Map<String, AttributeValue> dynamoRecord) {
        if (CollectionUtils.isNullOrEmpty(dynamoRecord)) {
            return null;
        } else {
            return new WorkerIdExclusionState(
                    getRegexAttribute(dynamoRecord),
                    getExpirationInstantAttribute(dynamoRecord),
                    getOnlyExcludingLeadershipAttribute(dynamoRecord));
        }
    }

    public static String getRegexAttribute(@NonNull Map<String, AttributeValue> dynamoRecord) {
        AttributeValue av = dynamoRecord.get(REGEX_HASH_KEY);
        if (av == null) {
            return DEFAULT_REGEX;
        } else {
            return av.s();
        }
    }

    public static Long getExpirationInstantAttribute(@NonNull Map<String, AttributeValue> dynamoRecord) {
        AttributeValue av = dynamoRecord.get(EXPIRATION_HASH_KEY);
        if (av == null) {
            return DEFAULT_EXPIRATION;
        } else {
            return Long.valueOf(av.n());
        }
    }

    public static boolean getOnlyExcludingLeadershipAttribute(@NonNull Map<String, AttributeValue> dynamoRecord) {
        AttributeValue av = dynamoRecord.get(ONLY_EXCLUDING_LEADERSHIP_HASH_KEY);
        if (av == null) {
            return DEFAULT_EXCLUDING_LEADERSHIP;
        } else {
            return av.bool();
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (this == null ^ other == null) {
            return false;
        } else if (!(other instanceof WorkerIdExclusionState)) {
            return false;
        } else {
            WorkerIdExclusionState o = (WorkerIdExclusionState) other;
            return sameRegex(o)
                    && this.expirationInstant.equals(o.getExpirationInstant())
                    && this.onlyExcludingLeadership == o.isOnlyExcludingLeadership();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.regex, this.expirationInstant, this.onlyExcludingLeadership);
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
