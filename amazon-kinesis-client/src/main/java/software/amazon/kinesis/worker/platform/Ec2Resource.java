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

package software.amazon.kinesis.worker.platform;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.VisibleForTesting;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

import static software.amazon.kinesis.worker.platform.OperatingRangeDataProvider.LINUX_PROC;

/**
 * Provides resource metadata for EC2.
 */
@KinesisClientInternalApi
@Slf4j
public class Ec2Resource implements ResourceMetadataProvider {
    // https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/retrieve-iid.html
    private static final String IMDS_URL = "http://169.254.169.254/latest/dynamic/instance-identity/document";
    private static final String TOKEN_URL = "http://169.254.169.254/latest/api/token";
    private static final int EC2_INSTANCE_METADATA_TIMEOUT_MILLIS = 5000;

    private final UrlOpener identityDocumentUrl;
    private final UrlOpener tokenUrl;

    @VisibleForTesting
    Ec2Resource(UrlOpener identityDocumentUrl, UrlOpener tokenUrl) {
        this.identityDocumentUrl = identityDocumentUrl;
        this.tokenUrl = tokenUrl;
    }

    /**
     * Factory method to create an instance of Ec2Resource.
     *
     * @return Ec2Resource instance
     */
    public static Ec2Resource create() {
        try {
            return new Ec2Resource(new UrlOpener(new URL(IMDS_URL)), new UrlOpener(new URL(TOKEN_URL)));
        } catch (MalformedURLException e) {
            // It should not throw unless it's unit testing.
            throw new IllegalArgumentException(e);
        }
    }

    private boolean isEc2() {
        try {
            final HttpURLConnection connection = identityDocumentUrl.openConnection();
            connection.setRequestMethod("GET");
            // IMDS v2 requires IMDS token
            connection.setRequestProperty("X-aws-ec2-metadata-token", fetchImdsToken());
            connection.setConnectTimeout(EC2_INSTANCE_METADATA_TIMEOUT_MILLIS);
            connection.setReadTimeout(EC2_INSTANCE_METADATA_TIMEOUT_MILLIS);
            if (connection.getResponseCode() == 200) {
                return true;
            }
        } catch (Exception e) {
            // TODO: probably need to add retries as well.
            log.error("Unable to retrieve instance metadata", e);
        }
        return false;
    }

    private String fetchImdsToken() {
        try {
            final HttpURLConnection connection = tokenUrl.openConnection();
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("X-aws-ec2-metadata-token-ttl-seconds", "600");
            connection.setConnectTimeout(EC2_INSTANCE_METADATA_TIMEOUT_MILLIS);
            connection.setReadTimeout(EC2_INSTANCE_METADATA_TIMEOUT_MILLIS);
            if (connection.getResponseCode() == 200) {
                return new BufferedReader(new InputStreamReader(tokenUrl.getInputStream(connection)))
                        .lines()
                        .collect(Collectors.joining());
            }
        } catch (Exception e) {
            log.warn(
                    "Unable to retrieve IMDS token. It could mean that the instance is not EC2 or is using IMDS V1", e);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOnPlatform() {
        return isEc2();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ComputePlatform getPlatform() {
        return ComputePlatform.EC2;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<OperatingRangeDataProvider> getOperatingRangeDataProvider() {
        return Optional.of(LINUX_PROC).filter(OperatingRangeDataProvider::isProvider);
    }
}
