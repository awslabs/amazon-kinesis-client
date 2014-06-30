/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;

import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;

/**
 * 
 * Defines an ordering on checkpoint values, taking into account sentinel values: TRIM_HORIZON, LATEST,
 * SHARD_END.
 * 
 * SHARD_END -> infinity
 * TRIM_HORIZON and LATEST -> less than sequence numbers
 * sequence numbers -> BigInteger value of string
 * 
 */
class CheckpointValueComparator implements Comparator<String>, Serializable {

    private static final long serialVersionUID = 1L;

    // Define TRIM_HORIZON and LATEST to be less than all sequence numbers
    private static final BigInteger TRIM_HORIZON_BIG_INTEGER_VALUE = BigInteger.valueOf(-2);
    private static final BigInteger LATEST_BIG_INTEGER_VALUE = BigInteger.valueOf(-1);

    /**
     * Constructor.
     */
    CheckpointValueComparator() {

    }

    /**
     * Compares checkpoint values with these rules.
     * 
     * SHARD_END is considered greatest
     * TRIM_HORIZON and LATEST are considered less than sequence numbers
     * sequence numbers are given their big integer value
     * 
     * @param first The first element to be compared
     * @param second The second element to be compared
     * @return returns negative/0/positive if first is less than/equal to/greater than second
     * @throws IllegalArgumentException If either input is a non-numeric non-sentinel value string.
     */
    @Override
    public int compare(String first, String second) {
        if (!isDigitsOrSentinelValue(first) || !isDigitsOrSentinelValue(second)) {
            throw new IllegalArgumentException("Expected a sequence number or a sentinel checkpoint value but "
                    + "received: first=" + first + " and second=" + second);
        }
        // SHARD_END is the greatest
        if (SentinelCheckpoint.SHARD_END.toString().equals(first)
                && SentinelCheckpoint.SHARD_END.toString().equals(second)) {
            return 0;
        } else if (SentinelCheckpoint.SHARD_END.toString().equals(second)) {
            return -1;
        } else if (SentinelCheckpoint.SHARD_END.toString().equals(first)) {
            return 1;
        }

        // Compare other sentinel values and serial numbers after converting them to a big integer value
        return bigIntegerValue(first).compareTo(bigIntegerValue(second));
    }

    /**
     * Sequence numbers are converted, sentinels are given a value of -1. Note this method is only used after special
     * logic associated with SHARD_END and the case of comparing two sentinel values has already passed, so we map
     * sentinel values LATEST and TRIM_HORIZON to negative numbers so that they are considered less than sequence
     * numbers.
     * 
     * @param checkpointValue string to convert to big integer value
     * @return a BigInteger value representation of the checkpointValue
     */
    private static BigInteger bigIntegerValue(String checkpointValue) {
        if (SequenceNumberValidator.isDigits(checkpointValue)) {
            return new BigInteger(checkpointValue);
        } else if (SentinelCheckpoint.LATEST.toString().equals(checkpointValue)) {
            return LATEST_BIG_INTEGER_VALUE;
        } else if (SentinelCheckpoint.TRIM_HORIZON.toString().equals(checkpointValue)) {
            return TRIM_HORIZON_BIG_INTEGER_VALUE;
        } else {
            throw new IllegalArgumentException("Expected a string of digits, TRIM_HORIZON, or LATEST but received "
                    + checkpointValue);
        }
    }

    /**
     * Checks if the string is all digits or one of the SentinelCheckpoint values.
     * 
     * @param string
     * @return true if and only if the string is all digits or one of the SentinelCheckpoint values
     */
    private static boolean isDigitsOrSentinelValue(String string) {
        return SequenceNumberValidator.isDigits(string) || isSentinelValue(string);
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
}
