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
package software.amazon.kinesis.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.util.Optional;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StreamARNUtil {

    /**
     * Caches an {@link Arn} constructed from a {@link StsClient#getCallerIdentity()} call.
     */
    private static final SupplierCache<Arn> CALLER_IDENTITY_ARN = new SupplierCache<>(() -> {
        try (final SdkHttpClient httpClient = UrlConnectionHttpClient.builder().build();
                final StsClient stsClient = StsClient.builder().httpClient(httpClient).build()) {
            final GetCallerIdentityResponse response = stsClient.getCallerIdentity();
            final Arn arn = Arn.fromString(response.arn());

            // guarantee the cached ARN will never have an empty accountId
            arn.accountId().orElseThrow(() -> new IllegalStateException("AccountId is not present on " + arn));
            return arn;
        } catch (AwsServiceException | SdkClientException e) {
            log.warn("Unable to get sts caller identity to build stream arn", e);
            return null;
        }
    });

    /**
     * Retrieves the stream ARN using the stream name, region, and accountId returned by STS.
     * It is designed to fail gracefully, returning Optional.empty() if any errors occur.
     *
     * @param streamName stream name
     * @param kinesisRegion Kinesis client endpoint, and also where the stream(s) to be
     *          processed are located. A null guarantees an empty ARN.
     */
    public static Optional<Arn> getStreamARN(String streamName, Region kinesisRegion) {
        return getStreamARN(streamName, kinesisRegion, null);
    }

    public static Optional<Arn> getStreamARN(String streamName, Region kinesisRegion, String accountId) {
        if (kinesisRegion == null) {
            return Optional.empty();
        }

        final Arn identityArn = CALLER_IDENTITY_ARN.get();
        if (identityArn == null) {
            return Optional.empty();
        }

        // the provided accountId takes precedence
        final String chosenAccountId = (accountId != null) ? accountId : identityArn.accountId().get();
        return Optional.of(Arn.builder()
                .partition(identityArn.partition())
                .service("kinesis")
                .region(kinesisRegion.toString())
                .accountId(chosenAccountId)
                .resource("stream/" + streamName)
                .build());
    }

}
