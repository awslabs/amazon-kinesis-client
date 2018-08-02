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
package com.amazonaws.services.kinesis.multilang.config;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Test;

public class DatePropertyValueDecoderTest {

    private DatePropertyValueDecoder decoder = new DatePropertyValueDecoder();

    private static final String TEST_VALUE = "1527267472";

    @Test
    public void testNumericValue() {
        Date timestamp = decoder.decodeValue(TEST_VALUE);
        assertEquals(timestamp.getClass(), Date.class);
        assertEquals(timestamp, new Date(Long.parseLong(TEST_VALUE) * 1000L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyValue() {
        Date timestamp = decoder.decodeValue("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullValue() {
        Date timestamp = decoder.decodeValue(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonNumericValue() {
        Date timestamp = decoder.decodeValue("123abc");
    }
}
