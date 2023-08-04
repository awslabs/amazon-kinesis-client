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
package software.amazon.kinesis.multilang.auth;

import java.util.Arrays;

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.Builder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;

import software.amazon.kinesis.multilang.NestedPropertyKey;
import software.amazon.kinesis.multilang.NestedPropertyProcessor;

/**
 * An {@link AWSSessionCredentialsProvider} that is backed by STSAssumeRole.
 */
public class KclSTSAssumeRoleSessionCredentialsProvider
        implements AWSSessionCredentialsProvider, NestedPropertyProcessor {

    private final Builder builder;

    private final STSAssumeRoleSessionCredentialsProvider provider;

    /**
     *
     * @param params vararg parameters which must include roleArn at index=0,
     *      and roleSessionName at index=1
     */
    public KclSTSAssumeRoleSessionCredentialsProvider(final String[] params) {
        this(params[0], params[1], Arrays.copyOfRange(params, 2, params.length));
    }

    public KclSTSAssumeRoleSessionCredentialsProvider(final String roleArn, final String roleSessionName,
            final String... params) {
        builder = new Builder(roleArn, roleSessionName);
        NestedPropertyKey.parse(this, params);
        provider = builder.build();
    }

    @Override
    public AWSSessionCredentials getCredentials() {
        return provider.getCredentials();
    }

    @Override
    public void refresh() {
        // do nothing
    }

    @Override
    public void acceptEndpoint(final String serviceEndpoint, final String signingRegion) {
        final EndpointConfiguration endpoint = new EndpointConfiguration(serviceEndpoint, signingRegion);
        final AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClient.builder()
                .withEndpointConfiguration(endpoint)
                .build();
        builder.withStsClient(stsClient);
    }

    @Override
    public void acceptEndpointRegion(final Regions region) {
        final AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClient.builder()
                .withRegion(region)
                .build();
        builder.withStsClient(stsClient);
    }

    @Override
    public void acceptExternalId(final String externalId) {
        builder.withExternalId(externalId);
    }

}