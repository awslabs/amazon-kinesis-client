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

import java.util.Arrays;

import lombok.ToString;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.kinesis.multilang.auth.KclStsAssumeRoleCredentialsProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class AwsCredentialsProviderPropertyValueDecoderTest {

    private static final String TEST_ACCESS_KEY_ID = "123";
    private static final String TEST_SECRET_KEY = "456";

    private final String credentialName1 = AlwaysSucceedCredentialsProvider.class.getName();
    private final String credentialName2 = ConstructorCredentialsProvider.class.getName();
    private final String createCredentialClass = CreateProvider.class.getName();
    private final AwsCredentialsProviderPropertyValueDecoder decoder = new AwsCredentialsProviderPropertyValueDecoder();

    @ToString
    private static class AwsCredentialsMatcher extends TypeSafeDiagnosingMatcher<AwsCredentialsProvider> {

        private final Matcher<String> akidMatcher;
        private final Matcher<String> secretMatcher;
        private final Matcher<Class<?>> classMatcher;

        public AwsCredentialsMatcher(String akid, String secret) {
            this.akidMatcher = equalTo(akid);
            this.secretMatcher = equalTo(secret);
            this.classMatcher = instanceOf(AwsCredentialsProviderChain.class);
        }

        @Override
        protected boolean matchesSafely(AwsCredentialsProvider item, Description mismatchDescription) {
            AwsCredentials actual = item.resolveCredentials();
            boolean matched = true;

            if (!classMatcher.matches(item)) {
                classMatcher.describeMismatch(item, mismatchDescription);
                matched = false;
            }

            if (!akidMatcher.matches(actual.accessKeyId())) {
                akidMatcher.describeMismatch(actual.accessKeyId(), mismatchDescription);
                matched = false;
            }
            if (!secretMatcher.matches(actual.secretAccessKey())) {
                secretMatcher.describeMismatch(actual.secretAccessKey(), mismatchDescription);
                matched = false;
            }
            return matched;
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("An AwsCredentialsProvider that provides an AwsCredential matching: ")
                    .appendList("(", ", ", ")", Arrays.asList(classMatcher, akidMatcher, secretMatcher));
        }
    }

    private static AwsCredentialsMatcher hasCredentials(String akid, String secret) {
        return new AwsCredentialsMatcher(akid, secret);
    }

    @Test
    public void testSingleProvider() {
        AwsCredentialsProvider provider = decoder.decodeValue(credentialName1);
        assertThat(provider, hasCredentials(TEST_ACCESS_KEY_ID, TEST_SECRET_KEY));
    }

    @Test
    public void testTwoProviders() {
        AwsCredentialsProvider provider = decoder.decodeValue(credentialName1 + "," + credentialName1);
        assertThat(provider, hasCredentials(TEST_ACCESS_KEY_ID, TEST_SECRET_KEY));
    }

    @Test
    public void testProfileProviderWithOneArg() {
        AwsCredentialsProvider provider = decoder.decodeValue(credentialName2 + "|arg");
        assertThat(provider, hasCredentials("arg", "blank"));
    }

    @Test
    public void testProfileProviderWithTwoArgs() {
        AwsCredentialsProvider provider = decoder.decodeValue(credentialName2 + "|arg1|arg2");
        assertThat(provider, hasCredentials("arg1", "arg2"));
    }

    /**
     * Test that providers in the multi-lang auth package can be resolved and instantiated.
     */
    @Test
    public void testKclAuthProvider() {
        for (final String className : Arrays.asList(
                KclStsAssumeRoleCredentialsProvider.class.getName(), // fully-qualified name
                KclStsAssumeRoleCredentialsProvider.class.getSimpleName(), // name-only; needs prefix
                StsAssumeRoleCredentialsProvider.class.getName(), // user passes full sts package path
                StsAssumeRoleCredentialsProvider.class.getSimpleName())) {
            final AwsCredentialsProvider provider =
                    decoder.decodeValue(className + "|arn|sessionName|endpointRegion=us-east-1");
            assertNotNull(className, provider);
        }
    }

    /**
     * Test that OneArgCreateProvider in the SDK v2 can process a create() method
     */
    @Test
    public void testEmptyCreateProvider() {
        AwsCredentialsProvider provider = decoder.decodeValue(createCredentialClass);
        assertThat(provider, hasCredentials(TEST_ACCESS_KEY_ID, TEST_SECRET_KEY));
    }

    /**
     * Test that OneArgCreateProvider in the SDK v2 can process a create(arg1) method
     */
    @Test
    public void testOneArgCreateProvider() {
        AwsCredentialsProvider provider = decoder.decodeValue(createCredentialClass + "|testCreateProperty");
        assertThat(provider, hasCredentials("testCreateProperty", TEST_SECRET_KEY));
    }

    /**
     * Test that a provider can be instantiated by its varargs constructor.
     */
    @Test
    public void testVarArgAuthProvider() {
        final String[] args = new String[] {"arg1", "arg2", "arg3"};
        final String className = VarArgCredentialsProvider.class.getName();
        final String encodedValue = className + "|" + String.join("|", args);

        final AwsCredentialsProvider provider = decoder.decodeValue(encodedValue);
        assertEquals(Arrays.toString(args), provider.resolveCredentials().accessKeyId());
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProvider implements AwsCredentialsProvider {
        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create(TEST_ACCESS_KEY_ID, TEST_SECRET_KEY);
        }
    }

    /**
     * This credentials provider needs a constructor call to instantiate it
     */
    public static class ConstructorCredentialsProvider implements AwsCredentialsProvider {

        private String arg1;
        private String arg2;

        @SuppressWarnings("unused")
        public ConstructorCredentialsProvider(String arg1) {
            this(arg1, "blank");
        }

        public ConstructorCredentialsProvider(String arg1, String arg2) {
            this.arg1 = arg1;
            this.arg2 = arg2;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create(arg1, arg2);
        }
    }

    private static class VarArgCredentialsProvider implements AwsCredentialsProvider {

        private final String[] args;

        public VarArgCredentialsProvider(final String[] args) {
            this.args = args;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            // KISS solution to surface the constructor args
            final String flattenedArgs = Arrays.toString(args);
            return AwsBasicCredentials.create(flattenedArgs, flattenedArgs);
        }
    }

    /**
     * Credentials provider to test AWS SDK v2 create() methods for providers like ProfileCredentialsProvider
     */
    public static class CreateProvider implements AwsCredentialsProvider {
        private String accessKeyId;

        private CreateProvider(String accessKeyId) {
            this.accessKeyId = accessKeyId;
        }

        public static CreateProvider create() {
            return new CreateProvider(TEST_ACCESS_KEY_ID);
        }

        public static CreateProvider create(String accessKeyId) {
            return new CreateProvider(accessKeyId);
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create(accessKeyId, TEST_SECRET_KEY);
        }
    }
}
