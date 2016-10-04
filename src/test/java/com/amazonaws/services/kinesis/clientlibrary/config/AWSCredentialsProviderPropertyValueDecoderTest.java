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
package com.amazonaws.services.kinesis.clientlibrary.config;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.config.AWSCredentialsProviderPropertyValueDecoder;

public class AWSCredentialsProviderPropertyValueDecoderTest {

    private static final String TEST_ACCESS_KEY_ID = "123";
    private static final String TEST_SECRET_KEY = "456";

    private String credentialName1 =
            "com.amazonaws.services.kinesis.clientlibrary.config.AWSCredentialsProviderPropertyValueDecoderTest$AlwaysSucceedCredentialsProvider";
    private String credentialName2 =
            "com.amazonaws.services.kinesis.clientlibrary.config.AWSCredentialsProviderPropertyValueDecoderTest$ConstructorCredentialsProvider";
    private AWSCredentialsProviderPropertyValueDecoder decoder = new AWSCredentialsProviderPropertyValueDecoder();

    @Test
    public void testSingleProvider() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName1);
        assertEquals(provider.getClass(), AWSCredentialsProviderChain.class);
        assertEquals(provider.getCredentials().getAWSAccessKeyId(), TEST_ACCESS_KEY_ID);
        assertEquals(provider.getCredentials().getAWSSecretKey(), TEST_SECRET_KEY);
    }

    @Test
    public void testTwoProviders() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName1 + "," + credentialName1);
        assertEquals(provider.getClass(), AWSCredentialsProviderChain.class);
        assertEquals(provider.getCredentials().getAWSAccessKeyId(), TEST_ACCESS_KEY_ID);
        assertEquals(provider.getCredentials().getAWSSecretKey(), TEST_SECRET_KEY);
    }

    @Test
    public void testProfileProviderWithOneArg() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName2 + "|arg");
        assertEquals(provider.getClass(), AWSCredentialsProviderChain.class);
        assertEquals(provider.getCredentials().getAWSAccessKeyId(), "arg");
        assertEquals(provider.getCredentials().getAWSSecretKey(), "blank");
    }

    @Test
    public void testProfileProviderWithTwoArgs() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName2 +
            "|arg1|arg2");
        assertEquals(provider.getClass(), AWSCredentialsProviderChain.class);
        assertEquals(provider.getCredentials().getAWSAccessKeyId(), "arg1");
        assertEquals(provider.getCredentials().getAWSSecretKey(), "arg2");
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
        public void refresh() {
        }

    }

    /**
     * This credentials provider needs a constructor call to instantiate it
     */
    public static class ConstructorCredentialsProvider implements AWSCredentialsProvider {

        private String arg1;
        private String arg2;

        public ConstructorCredentialsProvider(String arg1) {
            this.arg1 = arg1;
            this.arg2 = "blank";
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
        public void refresh() {
        }

    }
}
