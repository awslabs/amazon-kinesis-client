/*
 * Copyright 2023 Amazon.com, Inc. or its affiliates.
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

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FunctionCacheTest {

    @Mock
    private Function<Integer, Object> mockFunction;

    private FunctionCache<Integer, Object> cache;

    @Before
    public void setUp() {
        cache = new FunctionCache<>(mockFunction);
    }

    /**
     * Test that the cache stops invoking the encapsulated {@link Function}
     * after it returns a non-null value.
     */
    @Test
    public void testCache() {
        final int expectedValue = 3;
        when(mockFunction.apply(expectedValue)).thenReturn(expectedValue);

        assertNull(cache.get(1));
        assertNull(cache.get(2));
        assertEquals(expectedValue, cache.get(3));
        assertEquals(expectedValue, cache.get(4));
        assertEquals(expectedValue, cache.get(5));
        verify(mockFunction, times(expectedValue)).apply(anyInt());
    }
}