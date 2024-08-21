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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import lombok.ToString;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import software.amazon.kinesis.multilang.auth.KclSTSAssumeRoleSessionCredentialsProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class AWSCredentialsProviderPropertyValueDecoderTest {

    private static final String TEST_ACCESS_KEY_ID = "123";
    private static final String TEST_SECRET_KEY = "456";

    private final String credentialName1 = AlwaysSucceedCredentialsProvider.class.getName();
    private final String credentialName2 = ConstructorCredentialsProvider.class.getName();
    private final AWSCredentialsProviderPropertyValueDecoder decoder = new AWSCredentialsProviderPropertyValueDecoder();

    @ToString
    private static class AWSCredentialsMatcher extends TypeSafeDiagnosingMatcher<AWSCredentialsProvider> {

        private final Matcher<String> akidMatcher;
        private final Matcher<String> secretMatcher;
        private final Matcher<Class<?>> classMatcher;

        public AWSCredentialsMatcher(String akid, String secret) {
            this.akidMatcher = equalTo(akid);
            this.secretMatcher = equalTo(secret);
            this.classMatcher = instanceOf(AWSCredentialsProviderChain.class);
        }

        @Override
        protected boolean matchesSafely(AWSCredentialsProvider item, Description mismatchDescription) {
            AWSCredentials actual = item.getCredentials();
            boolean matched = true;

            if (!classMatcher.matches(item)) {
                classMatcher.describeMismatch(item, mismatchDescription);
                matched = false;
            }

            if (!akidMatcher.matches(actual.getAWSAccessKeyId())) {
                akidMatcher.describeMismatch(actual.getAWSAccessKeyId(), mismatchDescription);
                matched = false;
            }
            if (!secretMatcher.matches(actual.getAWSSecretKey())) {
                secretMatcher.describeMismatch(actual.getAWSSecretKey(), mismatchDescription);
                matched = false;
            }
            return matched;
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("An AWSCredentialsProvider that provides an AWSCredential matching: ")
                    .appendList("(", ", ", ")", Arrays.asList(classMatcher, akidMatcher, secretMatcher));
        }
    }

    private static AWSCredentialsMatcher hasCredentials(String akid, String secret) {
        return new AWSCredentialsMatcher(akid, secret);
    }

    @Test
    public void testSingleProvider() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName1);
        assertThat(provider, hasCredentials(TEST_ACCESS_KEY_ID, TEST_SECRET_KEY));
    }

    @Test
    public void testTwoProviders() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName1 + "," + credentialName1);
        assertThat(provider, hasCredentials(TEST_ACCESS_KEY_ID, TEST_SECRET_KEY));
    }

    @Test
    public void testProfileProviderWithOneArg() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName2 + "|arg");
        assertThat(provider, hasCredentials("arg", "blank"));
    }

    @Test
    public void testProfileProviderWithTwoArgs() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName2 + "|arg1|arg2");
        assertThat(provider, hasCredentials("arg1", "arg2"));
    }

    /**
     * Test that providers in the multi-lang auth package can be resolved and instantiated.
     */
    @Test
    public void testKclAuthProvider() {
        for (final String className : Arrays.asList(
                KclSTSAssumeRoleSessionCredentialsProvider.class.getName(), // fully-qualified name
                KclSTSAssumeRoleSessionCredentialsProvider.class.getSimpleName() // name-only; needs prefix
                )) {
            final AWSCredentialsProvider provider = decoder.decodeValue(className + "|arn|sessionName");
            assertNotNull(className, provider);
        }
    }

    /**
     * Test that a provider can be instantiated by its varargs constructor.
     */
    @Test
    public void testVarArgAuthProvider() {
        final String[] args = new String[] {"arg1", "arg2", "arg3"};
        final String className = VarArgCredentialsProvider.class.getName();
        final String encodedValue = className + "|" + String.join("|", args);

        final AWSCredentialsProvider provider = decoder.decodeValue(encodedValue);
        assertEquals(Arrays.toString(args), provider.getCredentials().getAWSAccessKeyId());
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProvider implements AWSCredentialsProvider {

        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials(TEST_ACCESS_KEY_ID, TEST_SECRET_KEY);
        }

        @Override
        public void refresh() {}
    }

    /**
     * This credentials provider needs a constructor call to instantiate it
     */
    public static class ConstructorCredentialsProvider implements AWSCredentialsProvider {

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
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials(arg1, arg2);
        }

        @Override
        public void refresh() {}
    }

    private static class VarArgCredentialsProvider implements AWSCredentialsProvider {

        private final String[] args;

        public VarArgCredentialsProvider(final String[] args) {
            this.args = args;
        }

        @Override
        public AWSCredentials getCredentials() {
            // KISS solution to surface the constructor args
            final String flattenedArgs = Arrays.toString(args);
            return new BasicAWSCredentials(flattenedArgs, flattenedArgs);
        }

        @Override
        public void refresh() {}
    }
}
