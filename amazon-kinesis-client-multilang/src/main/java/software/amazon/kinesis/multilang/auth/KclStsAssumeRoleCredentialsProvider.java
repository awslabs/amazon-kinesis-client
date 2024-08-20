package software.amazon.kinesis.multilang.auth;

import java.util.Arrays;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.kinesis.multilang.NestedPropertyKey;
import software.amazon.kinesis.multilang.NestedPropertyProcessor;

public class KclStsAssumeRoleCredentialsProvider implements AwsCredentialsProvider, NestedPropertyProcessor {
    private final String roleArn;
    private final String roleSessionName;
    private Region region;
    private String serviceEndpoint;
    private String externalId;

    public KclStsAssumeRoleCredentialsProvider(String[] params) {
        this(params[0], params[1], Arrays.copyOfRange(params, 2, params.length));
    }

    public KclStsAssumeRoleCredentialsProvider(String roleArn, String roleSessionName, String... params) {
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
        NestedPropertyKey.parse(this, params);
    }

    @Override
    public AwsCredentials resolveCredentials() {
        StsAssumeRoleCredentialsProviderConfig config = new StsAssumeRoleCredentialsProviderConfig(
                roleArn, roleSessionName, region, serviceEndpoint, externalId);
        StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider =
                StsAssumeRoleCredentialsProviderFactory.createProvider(config);
        return stsAssumeRoleCredentialsProvider.resolveCredentials();
    }

    @Override
    public void acceptEndpoint(String serviceEndpoint, String signingRegion) {
        this.serviceEndpoint = serviceEndpoint;
        this.region = Region.of(signingRegion);
    }

    @Override
    public void acceptEndpointRegion(Region region) {
        this.region = region;
    }

    @Override
    public void acceptExternalId(String externalId) {
        this.externalId = externalId;
    }
}
