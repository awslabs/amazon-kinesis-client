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
package software.amazon.kinesis.retrieval.kpl;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import software.amazon.kinesis.checkpoint.SentinelCheckpoint;

/**
 * Represents a two-part sequence number for records aggregated by the Kinesis
 * Producer Library.
 *
 * <p>
 * The KPL combines multiple user records into a single Kinesis record. Each
 * user record therefore has an integer sub-sequence number, in addition to the
 * regular sequence number of the Kinesis record. The sub-sequence number is
 * used to checkpoint within an aggregated record.
 */
@EqualsAndHashCode
public class ExtendedSequenceNumber implements Comparable<ExtendedSequenceNumber> {
    private final String sequenceNumber;
    private final long subSequenceNumber;

    // Define TRIM_HORIZON, LATEST, and AT_TIMESTAMP to be less than all sequence numbers
    private static final BigInteger TRIM_HORIZON_BIG_INTEGER_VALUE = BigInteger.valueOf(-2);
    private static final BigInteger LATEST_BIG_INTEGER_VALUE = BigInteger.valueOf(-1);
    private static final BigInteger AT_TIMESTAMP_BIG_INTEGER_VALUE = BigInteger.valueOf(-3);

    /**
     * Special value for LATEST.
     */
    public static final ExtendedSequenceNumber LATEST =
            new ExtendedSequenceNumber(SentinelCheckpoint.LATEST.toString());

    /**
     * Special value for SHARD_END.
     */
    public static final ExtendedSequenceNumber SHARD_END =
            new ExtendedSequenceNumber(SentinelCheckpoint.SHARD_END.toString());
    /**
     *
     * Special value for TRIM_HORIZON.
     */
    public static final ExtendedSequenceNumber TRIM_HORIZON =
            new ExtendedSequenceNumber(SentinelCheckpoint.TRIM_HORIZON.toString());

    /**
     * Special value for AT_TIMESTAMP.
     */
    public static final ExtendedSequenceNumber AT_TIMESTAMP =
            new ExtendedSequenceNumber(SentinelCheckpoint.AT_TIMESTAMP.toString());

    /**
     * Cache of {@link SentinelCheckpoint} values that avoids expensive
     * try-catch and Exception handling.
     *
     * @see #isSentinelCheckpoint()
     */
    private static final Set<String> SENTINEL_VALUES =
            Collections.unmodifiableSet(Arrays.stream(SentinelCheckpoint.values())
                    .map(SentinelCheckpoint::name)
                    .collect(Collectors.toSet()));

    /**
     * Construct an ExtendedSequenceNumber. The sub-sequence number defaults to
     * 0.
     *
     * @param sequenceNumber
     *            Sequence number of the Kinesis record
     */
    public ExtendedSequenceNumber(String sequenceNumber) {
        this(sequenceNumber, 0L);
    }

    /**
     * Construct an ExtendedSequenceNumber.
     *
     * @param sequenceNumber
     *            Sequence number of the Kinesis record
     * @param subSequenceNumber
     *            Sub-sequence number of the user record within the Kinesis
     *            record
     */
    public ExtendedSequenceNumber(String sequenceNumber, Long subSequenceNumber) {
        this.sequenceNumber = sequenceNumber;
        this.subSequenceNumber = subSequenceNumber == null ? 0L : subSequenceNumber;
    }

    /**
     * Compares this with another ExtendedSequenceNumber using these rules.
     *
     * SHARD_END is considered greatest
     * TRIM_HORIZON, LATEST and AT_TIMESTAMP are considered less than sequence numbers
     * sequence numbers are given their big integer value
     *
     * @param extendedSequenceNumber The ExtendedSequenceNumber to compare against
     * @return returns negative/0/positive if this is less than/equal to/greater than extendedSequenceNumber
     */
    @Override
    public int compareTo(ExtendedSequenceNumber extendedSequenceNumber) {
        String secondSequenceNumber = extendedSequenceNumber.sequenceNumber();

        if (!isDigitsOrSentinelValue(this) || !isDigitsOrSentinelValue(extendedSequenceNumber)) {
            throw new IllegalArgumentException("Expected a sequence number or a sentinel checkpoint value but "
                    + "received: first=" + sequenceNumber + " and second=" + secondSequenceNumber);
        }

        // SHARD_END is the greatest
        if (SentinelCheckpoint.SHARD_END.toString().equals(sequenceNumber)
                && SentinelCheckpoint.SHARD_END.toString().equals(secondSequenceNumber)) {
            return 0;
        } else if (SentinelCheckpoint.SHARD_END.toString().equals(secondSequenceNumber)) {
            return -1;
        } else if (SentinelCheckpoint.SHARD_END.toString().equals(sequenceNumber)) {
            return 1;
        }

        // Compare other sentinel values and serial numbers after converting them to a big integer value
        int result = bigIntegerValue(sequenceNumber).compareTo(bigIntegerValue(secondSequenceNumber));
        return result == 0 ? Long.compare(subSequenceNumber, extendedSequenceNumber.subSequenceNumber) : result;
    }

    /**
     *
     * @return The sequence number of the Kinesis record.
     */
    public String sequenceNumber() {
        return sequenceNumber;
    }

    /**
     *
     * @return The sub-sequence number of the user record within the enclosing
     *         Kinesis record.
     */
    public long subSequenceNumber() {
        return subSequenceNumber;
    }

    public boolean isShardEnd() {
        return sequenceNumber.equals(SentinelCheckpoint.SHARD_END.toString());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        if (sequenceNumber() != null) {
            sb.append("SequenceNumber: ").append(sequenceNumber()).append(',');
        }
        if (subSequenceNumber >= 0) {
            sb.append("SubsequenceNumber: ").append(subSequenceNumber());
        }
        sb.append('}');
        return sb.toString();
    }

    /**
     * Sequence numbers are converted, sentinels are given a value of -1. Note this method is only used after special
     * logic associated with SHARD_END and the case of comparing two sentinel values has already passed, so we map
     * sentinel values LATEST, TRIM_HORIZON and AT_TIMESTAMP to negative numbers so that they are considered less than
     * sequence numbers.
     *
     * @param sequenceNumber The string to convert to big integer value
     * @return a BigInteger value representation of the sequenceNumber
     */
    private static BigInteger bigIntegerValue(String sequenceNumber) {
        if (isDigits(sequenceNumber)) {
            return new BigInteger(sequenceNumber);
        } else if (SentinelCheckpoint.LATEST.toString().equals(sequenceNumber)) {
            return LATEST_BIG_INTEGER_VALUE;
        } else if (SentinelCheckpoint.TRIM_HORIZON.toString().equals(sequenceNumber)) {
            return TRIM_HORIZON_BIG_INTEGER_VALUE;
        } else if (SentinelCheckpoint.AT_TIMESTAMP.toString().equals(sequenceNumber)) {
            return AT_TIMESTAMP_BIG_INTEGER_VALUE;
        } else {
            throw new IllegalArgumentException("Expected a string of digits, TRIM_HORIZON, LATEST or AT_TIMESTAMP but "
                    + "received " + sequenceNumber);
        }
    }

    /**
     * Checks if a sequence number is all digits or a {@link SentinelCheckpoint}.
     *
     * @param esn {@code ExtendedSequenceNumber} to validate its sequence number
     * @return true if and only if the string is all digits or one of the SentinelCheckpoint values
     */
    private static boolean isDigitsOrSentinelValue(final ExtendedSequenceNumber esn) {
        return isDigits(esn.sequenceNumber()) || esn.isSentinelCheckpoint();
    }

    /**
     * Returns true if-and-only-if the sequence number is a {@link SentinelCheckpoint}.
     * Subsequence numbers are ignored when making this determination.
     */
    public boolean isSentinelCheckpoint() {
        return SENTINEL_VALUES.contains(sequenceNumber);
    }

    /**
     * Checks if the string is composed of only digits.
     *
     * @param string
     * @return true for a string of all digits, false otherwise (including false for null and empty string)
     */
    private static boolean isDigits(String string) {
        if (string == null || string.length() == 0) {
            return false;
        }
        for (int i = 0; i < string.length(); ++i) {
            if (!Character.isDigit(string.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
