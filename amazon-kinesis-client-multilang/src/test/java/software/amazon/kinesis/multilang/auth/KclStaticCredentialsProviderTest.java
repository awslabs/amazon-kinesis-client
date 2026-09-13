/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.multilang.auth;

import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KclStaticCredentialsProviderTest {

    private static final String ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE";
    private static final String SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    private static final String SESSION_TOKEN = "FwoGZXIvYXdzEBYaDI7example";

    @Test
    public void testConstructorWithTwoParametersArray() {
        KclStaticCredentialsProvider provider =
                new KclStaticCredentialsProvider(new String[] {ACCESS_KEY_ID, SECRET_ACCESS_KEY});

        AwsCredentials credentials = provider.resolveCredentials();
        assertNotNull(credentials);
        assertEquals(ACCESS_KEY_ID, credentials.accessKeyId());
        assertEquals(SECRET_ACCESS_KEY, credentials.secretAccessKey());
    }

    @Test
    public void testConstructorWithThreeParametersArray() {
        KclStaticCredentialsProvider provider =
                new KclStaticCredentialsProvider(new String[] {ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN});

        AwsCredentials credentials = provider.resolveCredentials();
        assertNotNull(credentials);
        assertEquals(ACCESS_KEY_ID, credentials.accessKeyId());
        assertEquals(SECRET_ACCESS_KEY, credentials.secretAccessKey());

        // Verify it's a session credential
        assertTrue(credentials instanceof AwsSessionCredentials);
        AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;
        assertEquals(SESSION_TOKEN, sessionCredentials.sessionToken());
    }

    @Test
    public void testConstructorWithTwoParameters() {
        KclStaticCredentialsProvider provider = new KclStaticCredentialsProvider(ACCESS_KEY_ID, SECRET_ACCESS_KEY);

        AwsCredentials credentials = provider.resolveCredentials();
        assertNotNull(credentials);
        assertEquals(ACCESS_KEY_ID, credentials.accessKeyId());
        assertEquals(SECRET_ACCESS_KEY, credentials.secretAccessKey());
    }

    @Test
    public void testConstructorWithThreeParameters() {
        KclStaticCredentialsProvider provider =
                new KclStaticCredentialsProvider(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);

        AwsCredentials credentials = provider.resolveCredentials();
        assertNotNull(credentials);
        assertEquals(ACCESS_KEY_ID, credentials.accessKeyId());
        assertEquals(SECRET_ACCESS_KEY, credentials.secretAccessKey());

        // Verify it's a session credential
        assertTrue(credentials instanceof AwsSessionCredentials);
        AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;
        assertEquals(SESSION_TOKEN, sessionCredentials.sessionToken());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullArray() {
        new KclStaticCredentialsProvider((String[]) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyArray() {
        new KclStaticCredentialsProvider(new String[] {});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithOneParameter() {
        new KclStaticCredentialsProvider(new String[] {ACCESS_KEY_ID});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithTooManyParameters() {
        new KclStaticCredentialsProvider(new String[] {ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN, "extra"});
    }

    @Test
    public void testToString() {
        KclStaticCredentialsProvider provider = new KclStaticCredentialsProvider(ACCESS_KEY_ID, SECRET_ACCESS_KEY);

        String result = provider.toString();
        assertNotNull(result);
        assertTrue(result.contains("KclStaticCredentialsProvider"));
    }

    @Test
    public void testResolveCredentialsMultipleTimes() {
        KclStaticCredentialsProvider provider = new KclStaticCredentialsProvider(ACCESS_KEY_ID, SECRET_ACCESS_KEY);

        // Verify that multiple calls return the same credentials
        AwsCredentials credentials1 = provider.resolveCredentials();
        AwsCredentials credentials2 = provider.resolveCredentials();

        assertNotNull(credentials1);
        assertNotNull(credentials2);
        assertEquals(credentials1.accessKeyId(), credentials2.accessKeyId());
        assertEquals(credentials1.secretAccessKey(), credentials2.secretAccessKey());
    }
}
