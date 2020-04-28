/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.common;

import com.google.common.base.Joiner;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;

@Data
@Accessors(fluent = true)
public class HashKeyRangeForLease {

    private static final String DELIM = ":";

    private final String startingHashKey;
    private final String endingHashKey;

    public String serialize() {
        return Joiner.on(DELIM).join(startingHashKey, endingHashKey);
    }

    @Override
    public String toString() {
        return serialize();
    }

    public static HashKeyRangeForLease deserialize(String hashKeyRange) {
        final String[] hashKeyTokens = hashKeyRange.split(DELIM);
        Validate.isTrue(hashKeyTokens.length == 2, "HashKeyRange should have exactly two tokens");
        // Assuming that startingHashKey and endingHashRange are not same.
        Validate.isTrue(!hashKeyTokens[0].equals(hashKeyTokens[1]), "StartingHashKey and EndingHashKey should not be same");
        Validate.isTrue(hashKeyTokens[0].compareTo(hashKeyTokens[1]) < 0, "StartingHashKey must be less than EndingHashKey");
        return new HashKeyRangeForLease(hashKeyTokens[0], hashKeyTokens[1]);
    }

    public static HashKeyRangeForLease fromHashKeyRange(HashKeyRange hashKeyRange) {
        return new HashKeyRangeForLease(hashKeyRange.startingHashKey(), hashKeyRange.endingHashKey());
    }
}
