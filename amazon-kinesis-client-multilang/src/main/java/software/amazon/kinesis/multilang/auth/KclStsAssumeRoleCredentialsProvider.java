package software.amazon.kinesis.multilang.auth;

import java.net.URI;
import java.util.Arrays;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest.Builder;
import software.amazon.kinesis.multilang.NestedPropertyKey;
import software.amazon.kinesis.multilang.NestedPropertyProcessor;

public class KclStsAssumeRoleCredentialsProvider implements AwsCredentialsProvider, NestedPropertyProcessor {
    private final Builder assumeRoleRequestBuilder;
    private final StsClientBuilder stsClientBuilder;
    private final StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider;

    public KclStsAssumeRoleCredentialsProvider(String[] params) {
        this(params[0], params[1], Arrays.copyOfRange(params, 2, params.length));
    }

    public KclStsAssumeRoleCredentialsProvider(String roleArn, String roleSessionName, String... params) {
        this.assumeRoleRequestBuilder =
                AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(roleSessionName);
        this.stsClientBuilder = StsClient.builder();
        NestedPropertyKey.parse(this, params);
        this.stsAssumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(assumeRoleRequestBuilder.build())
                .asyncCredentialUpdateEnabled(true)
                .stsClient(stsClientBuilder.build())
                .build();
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return stsAssumeRoleCredentialsProvider.resolveCredentials();
    }

    @Override
    public void acceptEndpoint(String serviceEndpoint, String signingRegion) {
        if (!serviceEndpoint.startsWith("http://") && !serviceEndpoint.startsWith("https://")) {
            serviceEndpoint = "https://" + serviceEndpoint;
        }
        stsClientBuilder.endpointOverride(URI.create(serviceEndpoint));
        stsClientBuilder.region(Region.of(signingRegion));
    }

    @Override
    public void acceptEndpointRegion(Region region) {
        stsClientBuilder.region(region);
    }

    @Override
    public void acceptExternalId(String externalId) {
        assumeRoleRequestBuilder.externalId(externalId);
    }
}
