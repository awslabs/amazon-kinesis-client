/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.retrieval.kpl;

import java.math.BigInteger;

//import com.amazonaws.services.kinesis.clientlibrary.lib.worker.String;
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
 * 
 * @author daphnliu
 *
 */
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
        this.subSequenceNumber = subSequenceNumber == null ? 0 : subSequenceNumber.longValue();
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

        if (!isDigitsOrSentinelValue(sequenceNumber) || !isDigitsOrSentinelValue(secondSequenceNumber)) {
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (sequenceNumber() != null) {
            sb.append("SequenceNumber: " + sequenceNumber() + ",");
        }
        if (subSequenceNumber >= 0) {
            sb.append("SubsequenceNumber: " + subSequenceNumber());
        }
        sb.append("}");
        return sb.toString();
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        final int shift = 32;
        int hashCode = 1;
        hashCode = prime * hashCode + ((sequenceNumber == null) ? 0 : sequenceNumber.hashCode());
        hashCode = prime * hashCode + ((subSequenceNumber < 0)
                ? 0
                : (int) (subSequenceNumber ^ (subSequenceNumber >>> shift)));
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof ExtendedSequenceNumber)) {
            return false;
        }
        ExtendedSequenceNumber other = (ExtendedSequenceNumber) obj;

        if (!sequenceNumber.equals(other.sequenceNumber())) {
            return false;
        }
        return subSequenceNumber == other.subSequenceNumber();
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
     * Checks if the string is all digits or one of the SentinelCheckpoint values.
     * 
     * @param string
     * @return true if and only if the string is all digits or one of the SentinelCheckpoint values
     */
    private static boolean isDigitsOrSentinelValue(String string) {
        return isDigits(string) || isSentinelValue(string);
    }

    /**
     * Checks if the string is a SentinelCheckpoint value.
     * 
     * @param string
     * @return true if and only if the string can be converted to a SentinelCheckpoint
     */
    private static boolean isSentinelValue(String string) {
        try {
            SentinelCheckpoint.valueOf(string);
            return true;
        } catch (Exception e) {
            return false;
        }
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
