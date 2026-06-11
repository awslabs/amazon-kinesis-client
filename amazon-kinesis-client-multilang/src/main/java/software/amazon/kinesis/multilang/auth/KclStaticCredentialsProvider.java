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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class KclStaticCredentialsProvider implements AwsCredentialsProvider {
    private final StaticCredentialsProvider delegate;

    public KclStaticCredentialsProvider(String[] params) {
        if (params == null || params.length < 2 || params.length > 3) {
            throw new IllegalArgumentException("KclStaticCredentialsProvider requires 2 or 3 parameters: "
                    + "accessKeyId, secretAccessKey, and optionally sessionToken. "
                    + "Received: "
                    + (params == null ? "null" : params.length + " parameters"));
        }

        this.delegate = params.length == 2
                ? createBasicCredentialsProvider(params[0], params[1])
                : createSessionCredentialsProvider(params[0], params[1], params[2]);
    }

    public KclStaticCredentialsProvider(String accessKeyId, String secretAccessKey) {
        this.delegate = createBasicCredentialsProvider(accessKeyId, secretAccessKey);
    }

    public KclStaticCredentialsProvider(String accessKeyId, String secretAccessKey, String sessionToken) {
        this.delegate = createSessionCredentialsProvider(accessKeyId, secretAccessKey, sessionToken);
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return delegate.resolveCredentials();
    }

    private static StaticCredentialsProvider createBasicCredentialsProvider(
            String accessKeyId, String secretAccessKey) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        return StaticCredentialsProvider.create(credentials);
    }

    private static StaticCredentialsProvider createSessionCredentialsProvider(
            String accessKeyId, String secretAccessKey, String sessionToken) {
        AwsSessionCredentials credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        return StaticCredentialsProvider.create(credentials);
    }

    @Override
    public String toString() {
        return "KclStaticCredentialsProvider{" + "delegate=" + delegate + '}';
    }
}
