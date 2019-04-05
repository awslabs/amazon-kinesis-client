/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.checkpoint;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * This supports extracting the shardId from a sequence number.
 *
 * <h2>Warning</h2>
 * <strong>Sequence numbers are an opaque value used by Kinesis, and maybe changed at any time. Should validation stop
 * working you may need to update your version of the KCL</strong>
 *
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

            //
            // If the bit length of the sequence number isn't 186 it's impossible for the version numbers
            // to be where we expect them. We treat this the same as an unknown version of the sequence number
            //
            // If the sequence number length isn't what we expect it's due to a new version of the sequence number or
            // an invalid sequence number. This
            //
            if (sequenceNumber.bitLength() != EXPECTED_BIT_LENGTH) {
                return Optional.empty();
            }

            //
            // Read the 4 most significant bits of the sequence number, the 2 most significant bits are implicitly 0
            // (2 == 0b0011). If the version number doesn't match we give up and say we can't parse the sequence number
            //
            int version = readOffset(sequenceNumber, VERSION_OFFSET, VERSION_MASK);
            if (version != VERSION) {
                return Optional.empty();
            }

            //
            // If we get here the sequence number is big enough, and the version matches so the shardId should be valid.
            //
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
     * <p>
     * <strong>This will return an empty Optional if the it's unable to extract the version number. This can occur for
     * multiple reasons including:
     * <ul>
     * <li>Kinesis has started using a new version of sequence numbers</li>
     * <li>The provided sequence number isn't a valid Kinesis sequence number.</li>
     * </ul>
     * </strong>
     * </p>
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
     * <strong>This will return an empty Optional if the sequence number isn't recognized. This can occur for multiple
     * reasons including:
     * <ul>
     * <li>Kinesis has started using a new version of sequence numbers</li>
     * <li>The provided sequence number isn't a valid Kinesis sequence number.</li>
     * </ul>
     * </strong>
     * <p>
     * This should always return a value if {@link #versionFor(String)} returns a value
     * </p>
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
     * <p>
     * Validation of a sequence number will only occur if the sequence number can be parsed. It's possible to use
     * {@link #versionFor(String)} to verify that the given sequence number is supported by this class. There are 3
     * possible validation states:
     * <dl>
     * <dt>Some(True)</dt>
     * <dd>The sequence number can be parsed, and the shardId matches the one in the sequence number</dd>
     * <dt>Some(False)</dt>
     * <dd>THe sequence number can be parsed, and the shardId doesn't match the one in the sequence number</dd>
     * <dt>None</dt>
     * <dd>It wasn't possible to parse the sequence number so the validity of the sequence number is unknown</dd>
     * </dl>
     * </p>
     *
     * <p>
     * <strong>Handling unknown validation causes is application specific, and not specific handling is
     * provided.</strong>
     * </p>
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
