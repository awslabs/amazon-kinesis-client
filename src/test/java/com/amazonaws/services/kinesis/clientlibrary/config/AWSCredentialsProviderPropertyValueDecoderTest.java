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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Set;
import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.config.AWSCredentialsProviderPropertyValueDecoder;

public class AWSCredentialsProviderPropertyValueDecoderTest {

    private String credentialName1 =
            "com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProvider";
    private String credentialName2 =
            "com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfiguratorTest$AlwaysFailCredentialsProvider";
    private AWSCredentialsProviderPropertyValueDecoder decoder = new AWSCredentialsProviderPropertyValueDecoder();

    @Test
    public void testSingleProvider() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName1);
        assertEquals(provider.getClass(), AWSCredentialsProviderChain.class);
    }

    @Test
    public void testTwoProviders() {
        AWSCredentialsProvider provider = decoder.decodeValue(credentialName1 + "," + credentialName2);
        assertEquals(provider.getClass(), AWSCredentialsProviderChain.class);
    }

    @Test
    public void testProfileProviderWithOneArg() {
        AWSCredentialsProvider provider = decoder.decodeValue(ProfileCredentialsProvider.class.getName() + "|profileName");
        assertEquals(provider.getClass(), AWSCredentialsProviderChain.class);
    }

    @Test
    public void testProfileProviderWithTwoArgs() throws IOException {
        File temp = File.createTempFile("test-profiles-file", ".tmp");
        temp.deleteOnExit();
        AWSCredentialsProvider provider = decoder.decodeValue(ProfileCredentialsProvider.class.getName() +
                "|" + temp.getAbsolutePath() + "|profileName");
        assertEquals(provider.getClass(), AWSCredentialsProviderChain.class);
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProvider implements AWSCredentialsProvider {

        @Override
        public AWSCredentials getCredentials() {
            return null;
        }

        @Override
        public void refresh() {
        }

    }

    /**
     * This credentials provider will always fail
     */
    public static class AlwaysFailCredentialsProvider implements AWSCredentialsProvider {

        @Override
        public AWSCredentials getCredentials() {
            throw new IllegalArgumentException();
        }

        @Override
        public void refresh() {
        }

    }
}