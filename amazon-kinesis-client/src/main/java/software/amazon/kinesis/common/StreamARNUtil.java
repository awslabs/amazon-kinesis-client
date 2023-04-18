package software.amazon.kinesis.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
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
            return Arn.fromString(response.arn());
        } catch (AwsServiceException | SdkClientException e) {
            log.warn("Unable to get sts caller identity to build stream arn", e);
            return null;
        }
    });

    /**
     * This static method attempts to retrieve the stream ARN using the stream name, region, and accountId returned by STS
     * It is designed to fail gracefully, returning Optional.empty() if any errors occur.
     *
     * @param streamName stream name
     * @param kinesisRegion kinesisRegion is a nullable parameter used to construct the stream arn
     */
    public static Optional<Arn> getStreamARN(String streamName, Region kinesisRegion) {
        return getStreamARN(streamName, kinesisRegion, Optional.empty());
    }

    public static Optional<Arn> getStreamARN(String streamName, Region kinesisRegion, @NonNull Optional<String> accountId) {
        if (kinesisRegion == null) {
            return Optional.empty();
        }

        final Arn identityArn = CALLER_IDENTITY_ARN.get();
        if (identityArn == null) {
            return Optional.empty();
        }

        // the provided accountId takes precedence
        final String chosenAccountId = accountId.orElse(identityArn.accountId().orElse(""));
        return Optional.of(Arn.builder()
                .partition(identityArn.partition())
                .service("kinesis")
                .region(kinesisRegion.toString())
                .accountId(chosenAccountId)
                .resource("stream/" + streamName)
                .build());
    }

}
