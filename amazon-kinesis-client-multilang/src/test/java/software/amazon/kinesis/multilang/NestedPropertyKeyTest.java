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
package software.amazon.kinesis.multilang;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static software.amazon.kinesis.multilang.NestedPropertyKey.ENDPOINT;
import static software.amazon.kinesis.multilang.NestedPropertyKey.ENDPOINT_REGION;
import static software.amazon.kinesis.multilang.NestedPropertyKey.EXTERNAL_ID;
import static software.amazon.kinesis.multilang.NestedPropertyKey.parse;

@RunWith(MockitoJUnitRunner.class)
public class NestedPropertyKeyTest {

    @Mock
    private NestedPropertyProcessor mockProcessor;

    @Test
    public void testExternalId() {
        final String expectedId = "eid";

        parse(mockProcessor, createKey(EXTERNAL_ID, expectedId));
        verify(mockProcessor).acceptExternalId(expectedId);
    }

    @Test
    public void testEndpoint() {
        final String expectedEndpoint = "https://sts.us-east-1.amazonaws.com";
        final String expectedRegion = "us-east-1";
        final String param = createKey(ENDPOINT, expectedEndpoint + "^" + expectedRegion);

        parse(mockProcessor, param);
        verify(mockProcessor).acceptEndpoint(expectedEndpoint, expectedRegion);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEndpoint() {
        parse(mockProcessor, createKey(ENDPOINT, "value-sans-caret-delimiter"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEndpointDoubleCaret() {
        parse(mockProcessor, createKey(ENDPOINT, "https://sts.us-east-1.amazonaws.com^us-east-1^borkbork"));
    }

    @Test
    public void testEndpointRegion() {
        final Region expectedRegion = Region.US_GOV_WEST_1;

        parse(mockProcessor, createKey(ENDPOINT_REGION, expectedRegion.id()));
        verify(mockProcessor).acceptEndpointRegion(expectedRegion);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEndpointRegion() {
        parse(mockProcessor, createKey(ENDPOINT_REGION, "snuffleupagus"));
    }

    /**
     * Test that the literal nested key (i.e., {@code key=} in {@code some_val|key=nested_val})
     * does not change. Any change to an existing literal key is not backwards-compatible.
     */
    @Test
    public void testKeysExplicitly() {
        // Adding a new enum will deliberately cause this assert to fail, and
        // therefore raise awareness for this explicit test. Add-and-remove may
        // keep the number unchanged yet will also break (by removing an enum).
        assertEquals(3, NestedPropertyKey.values().length);

        assertEquals("endpoint", ENDPOINT.getNestedKey());
        assertEquals("endpointRegion", ENDPOINT_REGION.getNestedKey());
        assertEquals("externalId", EXTERNAL_ID.getNestedKey());
    }

    @Test
    public void testNonmatchingParameters() {
        final String[] params = new String[] {
            null,
            "",
            "hello world", // no nested key
            "foo=bar", // nested key, but is not a recognized key
            createKey(EXTERNAL_ID, "eid") + "=extra", // valid key made invalid by second '='
        };
        parse(mockProcessor, params);
        verifyZeroInteractions(mockProcessor);
    }

    private static String createKey(final NestedPropertyKey key, final String value) {
        return key.getNestedKey() + "=" + value;
    }
}
