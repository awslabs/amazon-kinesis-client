package software.amazon.kinesis.common;

import com.google.common.base.Joiner;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.util.HashMap;
import java.util.Optional;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StreamARNUtil {
    private static final HashMap<String, Arn> streamARNCache = new HashMap<>();

    /**
     * This static method attempts to retrieve the stream ARN using the stream name, region, and accountId returned by STS
     * It is designed to fail gracefully, returning Optional.empty() if any errors occur.
     * @param streamName: stream name
     * @param kinesisRegion: kinesisRegion is a nullable parameter used to construct the stream arn
     * @return
     */
    public static Optional<Arn> getStreamARN(String streamName, Region kinesisRegion) {
        return getStreamARN(streamName, kinesisRegion, Optional.empty());
    }

    public static Optional<Arn> getStreamARN(String streamName, Region kinesisRegion, @NonNull Optional<String> accountId) {
        if (kinesisRegion == null || StringUtils.isEmpty(kinesisRegion.toString())) {
            return Optional.empty();
        }
        // Consult the cache before contacting STS
        String key = getCacheKey(streamName, kinesisRegion, accountId);
        if (streamARNCache.containsKey(key)) {
            return Optional.of(streamARNCache.get(key));
        }

        Optional<Arn> stsCallerArn = getStsCallerArn();
        if (!stsCallerArn.isPresent() || !stsCallerArn.get().accountId().isPresent()) {
            return Optional.empty();
        }
        accountId = accountId.isPresent() ? accountId : stsCallerArn.get().accountId();
        Arn kinesisStreamArn = Arn.builder()
                .partition(stsCallerArn.get().partition())
                .service("kinesis")
                .region(kinesisRegion.toString())
                .accountId(accountId.get())
                .resource("stream/" + streamName)
                .build();

        // Update the cache
        streamARNCache.put(key, kinesisStreamArn);
        return Optional.of(kinesisStreamArn);
    }

    private static Optional<Arn> getStsCallerArn() {
        GetCallerIdentityResponse response;
        try {
            response = getStsClient().getCallerIdentity();
        } catch (AwsServiceException | SdkClientException e) {
            log.warn("Unable to get sts caller identity to build stream arn", e);
            return Optional.empty();
        }
        return Optional.of(Arn.fromString(response.arn()));
    }

    private static StsClient getStsClient() {
        return StsClient.builder()
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
    }

    private static String getCacheKey(
            String streamName, @NonNull Region kinesisRegion, @NonNull Optional<String> accountId) {
        return Joiner.on(":").join(streamName, kinesisRegion.toString(), accountId.orElse(""));
    }

}
