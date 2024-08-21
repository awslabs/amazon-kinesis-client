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

import java.math.BigInteger;

import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;

/**
 * Lease POJO to hold the starting hashkey range and ending hashkey range of kinesis shards.
 */
@Accessors(fluent = true)
@Value
public class HashKeyRangeForLease {

    private final BigInteger startingHashKey;
    private final BigInteger endingHashKey;

    public HashKeyRangeForLease(BigInteger startingHashKey, BigInteger endingHashKey) {
        Validate.isTrue(
                startingHashKey.compareTo(endingHashKey) < 0,
                "StartingHashKey %s must be less than EndingHashKey %s ",
                startingHashKey,
                endingHashKey);
        this.startingHashKey = startingHashKey;
        this.endingHashKey = endingHashKey;
    }

    /**
     * Serialize the startingHashKey for persisting in external storage
     *
     * @return Serialized startingHashKey
     */
    public String serializedStartingHashKey() {
        return startingHashKey.toString();
    }

    /**
     * Serialize the endingHashKey for persisting in external storage
     *
     * @return Serialized endingHashKey
     */
    public String serializedEndingHashKey() {
        return endingHashKey.toString();
    }

    /**
     * Deserialize from serialized hashKeyRange string from external storage.
     *
     * @param startingHashKeyStr
     * @param endingHashKeyStr
     * @return HashKeyRangeForLease
     */
    public static HashKeyRangeForLease deserialize(
            @NonNull String startingHashKeyStr, @NonNull String endingHashKeyStr) {
        final BigInteger startingHashKey = new BigInteger(startingHashKeyStr);
        final BigInteger endingHashKey = new BigInteger(endingHashKeyStr);
        Validate.isTrue(
                startingHashKey.compareTo(endingHashKey) < 0,
                "StartingHashKey %s must be less than EndingHashKey %s ",
                startingHashKeyStr,
                endingHashKeyStr);
        return new HashKeyRangeForLease(startingHashKey, endingHashKey);
    }

    /**
     * Construct HashKeyRangeForLease from Kinesis HashKeyRange
     *
     * @param hashKeyRange
     * @return HashKeyRangeForLease
     */
    public static HashKeyRangeForLease fromHashKeyRange(HashKeyRange hashKeyRange) {
        return deserialize(hashKeyRange.startingHashKey(), hashKeyRange.endingHashKey());
    }
}
