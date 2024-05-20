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

package software.amazon.kinesis.multilang.config.credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

@RequiredArgsConstructor
public class V2CredentialWrapper implements AwsCredentialsProvider {

    private final AWSCredentialsProvider oldCredentialsProvider;

    @Override
    public AwsCredentials resolveCredentials() {
        AWSCredentials current = oldCredentialsProvider.getCredentials();
        if (current instanceof AWSSessionCredentials) {
            return AwsSessionCredentials.create(
                    current.getAWSAccessKeyId(),
                    current.getAWSSecretKey(),
                    ((AWSSessionCredentials) current).getSessionToken());
        }
        return new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return current.getAWSAccessKeyId();
            }

            @Override
            public String secretAccessKey() {
                return current.getAWSSecretKey();
            }
        };
    }
}
