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
package com.amazonaws.services.kinesis.clientlibrary.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Date;

import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.config.DatePropertyValueDecoder;

public class DatePropertyValueDecoderTest {

    private DatePropertyValueDecoder decoder = new DatePropertyValueDecoder();

    private static final String TEST_VALUE = "1527267472";

    @Test
    public void testNumericValue() {
        Date timestamp = decoder.decodeValue(TEST_VALUE);
        assertEquals(timestamp.getClass(), Date.class);
        assertEquals(timestamp, new Date(Long.parseLong(TEST_VALUE) * 1000L));
    }

    @Test
    public void testEmptyValue() {
        try {
          Date timestamp = decoder.decodeValue("");
          fail("Expect IllegalArgumentException on empty value");
        } catch (IllegalArgumentException e) {
          // success
        }
    }

    @Test
    public void testNullValue() {
        try {
          Date timestamp = decoder.decodeValue(null);
          fail("Expect IllegalArgumentException on null value");
        } catch (IllegalArgumentException e) {
          // success
        }
    }

    @Test
    public void testNonNumericValue() {
        try {
          Date timestamp = decoder.decodeValue("123abc");
          fail("Expect IllegalArgumentException on non numeric value");
        } catch (IllegalArgumentException e) {
          // success
        }
    }
}
