/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.fail;

import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

public class SequenceNumberValidatorTest {

    private final boolean validateWithGetIterator = true;
    private final String shardId = "shardid-123";

    @Test
    public final void testSequenceNumberValidator() {

        IKinesisProxy proxy = Mockito.mock(IKinesisProxy.class);

        SequenceNumberValidator validator = new SequenceNumberValidator(proxy, shardId, validateWithGetIterator);

        String goodSequence = "456";
        String iterator = "happyiterator";
        String badSequence = "789";
        Mockito.doReturn(iterator)
                .when(proxy)
                .getIterator(shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), goodSequence);
        Mockito.doThrow(new InvalidArgumentException(""))
                .when(proxy)
                .getIterator(shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), badSequence);

        validator.validateSequenceNumber(goodSequence);
        Mockito.verify(proxy, Mockito.times(1)).getIterator(shardId,
                ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(),
                goodSequence);

        try {
            validator.validateSequenceNumber(badSequence);
            fail("Bad sequence number did not cause the validator to throw an exception");
        } catch (IllegalArgumentException e) {
            Mockito.verify(proxy, Mockito.times(1)).getIterator(shardId,
                    ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(),
                    badSequence);
        }

        nonNumericValueValidationTest(validator, proxy, validateWithGetIterator);
    }

    @Test
    public final void testNoValidation() {
        IKinesisProxy proxy = Mockito.mock(IKinesisProxy.class);
        String shardId = "shardid-123";
        SequenceNumberValidator validator = new SequenceNumberValidator(proxy, shardId, !validateWithGetIterator);
        String goodSequence = "456";

        // Just checking that the false flag for validating against getIterator is honored
        validator.validateSequenceNumber(goodSequence);
        Mockito.verify(proxy, Mockito.times(0)).getIterator(shardId,
                ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(),
                goodSequence);

        // Validator should still validate sentinel values
        nonNumericValueValidationTest(validator, proxy, !validateWithGetIterator);
    }

    private void nonNumericValueValidationTest(SequenceNumberValidator validator,
            IKinesisProxy proxy,
            boolean validateWithGetIterator) {

        String[] nonNumericStrings = { null, "bogus-sequence-number", SentinelCheckpoint.LATEST.toString(),
                SentinelCheckpoint.TRIM_HORIZON.toString(),
                SentinelCheckpoint.AT_TIMESTAMP.toString() };

        for (String nonNumericString : nonNumericStrings) {
            try {
                validator.validateSequenceNumber(nonNumericString);
                fail("Validator should not consider " + nonNumericString + " a valid sequence number");
            } catch (IllegalArgumentException e) {
                // Non-numeric strings should always be rejected by the validator before the proxy can be called so we
                // check that the proxy was not called at all
                Mockito.verify(proxy, Mockito.times(0)).getIterator(shardId,
                        ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(),
                        nonNumericString);
            }
        }
    }

    @Test
    public final void testIsDigits() {
        // Check things that are all digits
        String[] stringsOfDigits = {
                "0",
                "12",
                "07897803434",
                "12324456576788",
        };
        for (String digits : stringsOfDigits) {
            Assert.assertTrue("Expected that " + digits + " would be considered a string of digits.",
                    SequenceNumberValidator.isDigits(digits));
        }
        // Check things that are not all digits
        String[] stringsWithNonDigits = {
                null,
                "",
                "      ", // white spaces
                "6 4",
                "\t45",
                "5242354235234\n",
                "7\n6\n5\n",
                "12s", // last character
                "c07897803434", // first character
                "1232445wef6576788", // interior
                "no-digits",
        };
        for (String notAllDigits : stringsWithNonDigits) {
            Assert.assertFalse("Expected that " + notAllDigits + " would not be considered a string of digits.",
                    SequenceNumberValidator.isDigits(notAllDigits));
        }
    }
}
