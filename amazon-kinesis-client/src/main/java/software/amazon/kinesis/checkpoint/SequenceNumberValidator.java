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

package software.amazon.kinesis.checkpoint;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * This supports extracting the shardId from a sequence number. The sequence number is generally an internal opaque type
 * used by Kinesis, and may change at any time.
 */
public class SequenceNumberValidator {

    @Data
    @Accessors(fluent = true)
    private static class SequenceNumberComponents {
        final int version;
        final int shardId;
    }

    private interface SequenceNumberReader {
        Optional<SequenceNumberComponents> read(String sequenceNumber);
    }

    /**
     * Reader for the v2 sequence number format. v1 sequence numbers are no longer used or available.
     */
    private static class V2SequenceNumberReader implements SequenceNumberReader {

        private static final int VERSION = 2;

        private static final int EXPECTED_BIT_LENGTH = 186;

        private static final int VERSION_OFFSET = 184;
        private static final long VERSION_MASK = (1 << 4) - 1;

        private static final int SHARD_ID_OFFSET = 4;
        private static final long SHARD_ID_MASK = (1L << 32) - 1;

        @Override
        public Optional<SequenceNumberComponents> read(String sequenceNumberString) {
            BigInteger sequenceNumber = new BigInteger(sequenceNumberString, 10);
            if (sequenceNumber.bitLength() != EXPECTED_BIT_LENGTH) {
                return Optional.empty();
            }
            int version = readOffset(sequenceNumber, VERSION_OFFSET, VERSION_MASK);
            if (version != VERSION) {
                return Optional.empty();
            }
            int shardId = readOffset(sequenceNumber, SHARD_ID_OFFSET, SHARD_ID_MASK);
            return Optional.of(new SequenceNumberComponents(version, shardId));
        }

        private int readOffset(BigInteger sequenceNumber, int offset, long mask) {
            long value = sequenceNumber.shiftRight(offset).longValue() & mask;
            return (int) value;
        }
    }

    private static final List<SequenceNumberReader> SEQUENCE_NUMBER_READERS = Collections
            .singletonList(new V2SequenceNumberReader());

    private Optional<SequenceNumberComponents> retrieveComponentsFor(String sequenceNumber) {
        return SEQUENCE_NUMBER_READERS.stream().map(r -> r.read(sequenceNumber)).filter(Optional::isPresent).map(Optional::get).findFirst();
    }

    /**
     * Attempts to retrieve the version for a sequence number. If no reader can be found for the sequence number this
     * will return an empty Optional.
     * 
     * @param sequenceNumber
     *            the sequence number to extract the version from
     * @return an Optional containing the version if a compatible sequence number reader can be found, an empty Optional
     *         otherwise.
     */
    public Optional<Integer> versionFor(String sequenceNumber) {
        return retrieveComponentsFor(sequenceNumber).map(SequenceNumberComponents::version);
    }

    /**
     * Attempts to retrieve the shardId from a sequence number. If the version of the sequence number is unsupported
     * this will return an empty optional.
     * 
     * @param sequenceNumber
     *            the sequence number to extract the shardId from
     * @return an Optional containing the shardId if the version is supported, an empty Optional otherwise.
     */
    public Optional<String> shardIdFor(String sequenceNumber) {
        return retrieveComponentsFor(sequenceNumber).map(s -> String.format("shardId-%012d", s.shardId()));
    }

    /**
     * Validates that the sequence number provided contains the given shardId. If the sequence number is unsupported
     * this will return an empty Optional.
     * 
     * @param sequenceNumber
     *            the sequence number to verify the shardId
     * @param shardId
     *            the shardId that the sequence is expected to contain
     * @return true if the sequence number contains the shardId, false if it doesn't. If the sequence number version is
     *         unsupported this will return an empty Optional
     */
    public Optional<Boolean> validateSequenceNumberForShard(String sequenceNumber, String shardId) {
        return shardIdFor(sequenceNumber).map(s -> StringUtils.equalsIgnoreCase(s, shardId));
    }

}
