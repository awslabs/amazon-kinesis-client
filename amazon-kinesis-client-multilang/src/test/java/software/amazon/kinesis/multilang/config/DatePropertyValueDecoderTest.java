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
package software.amazon.kinesis.multilang.config;

import java.util.Date;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
