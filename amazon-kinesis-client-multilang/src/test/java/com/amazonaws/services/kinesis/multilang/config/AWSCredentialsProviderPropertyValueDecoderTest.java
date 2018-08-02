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

import org.junit.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;

public class AWSCredentialsProviderPropertyValueDecoderTest {

    private static final String TEST_ACCESS_KEY_ID = "123";
    private static final String TEST_SECRET_KEY = "456";

    private String credentialName1 = "com.amazonaws.services.kinesis.multilang.config.AWSCredentialsProviderPropertyValueDecoderTest$AlwaysSucceedCredentialsProvider";
    private String credentialName2 = "com.amazonaws.services.kinesis.multilang.config.AWSCredentialsProviderPropertyValueDecoderTest$ConstructorCredentialsProvider";
    private AWSCredentialsProviderPropertyValueDecoder decoder = new AWSCredentialsProviderPropertyValueDecoder();

    @Test
    public void testSingleProvider() {
        AwsCredentialsProvider provider = decoder.decodeValue(credentialName1);
        assertEquals(provider.getClass(), AwsCredentialsProviderChain.class);
        assertEquals(provider.resolveCredentials().accessKeyId(), TEST_ACCESS_KEY_ID);
        assertEquals(provider.resolveCredentials().secretAccessKey(), TEST_SECRET_KEY);
    }

    @Test
    public void testTwoProviders() {
        AwsCredentialsProvider provider = decoder.decodeValue(credentialName1 + "," + credentialName1);
        assertEquals(provider.getClass(), AwsCredentialsProviderChain.class);
        assertEquals(provider.resolveCredentials().accessKeyId(), TEST_ACCESS_KEY_ID);
        assertEquals(provider.resolveCredentials().secretAccessKey(), TEST_SECRET_KEY);
    }

    @Test
    public void testProfileProviderWithOneArg() {
        AwsCredentialsProvider provider = decoder.decodeValue(credentialName2 + "|arg");
        assertEquals(provider.getClass(), AwsCredentialsProviderChain.class);
        assertEquals(provider.resolveCredentials().accessKeyId(), "arg");
        assertEquals(provider.resolveCredentials().secretAccessKey(), "blank");
    }

    @Test
    public void testProfileProviderWithTwoArgs() {
        AwsCredentialsProvider provider = decoder.decodeValue(credentialName2 + "|arg1|arg2");
        assertEquals(provider.getClass(), AwsCredentialsProviderChain.class);
        assertEquals(provider.resolveCredentials().accessKeyId(), "arg1");
        assertEquals(provider.resolveCredentials().secretAccessKey(), "arg2");
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

        public ConstructorCredentialsProvider(String arg1) {
            this.arg1 = arg1;
            this.arg2 = "blank";
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
}
