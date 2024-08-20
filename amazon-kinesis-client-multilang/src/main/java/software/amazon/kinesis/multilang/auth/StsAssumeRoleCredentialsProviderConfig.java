package software.amazon.kinesis.multilang.auth;

import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.regions.Region;

@Getter
@Setter
public class StsAssumeRoleCredentialsProviderConfig {
    private final String roleArn;
    private final String roleSessionName;
    private final Region region;
    private final String serviceEndpoint;
    private final String externalId;

    public StsAssumeRoleCredentialsProviderConfig(
            String roleArn, String roleSessionName, Region region, String serviceEndpoint, String externalId) {
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
        this.region = region;
        this.serviceEndpoint = serviceEndpoint;
        this.externalId = externalId;
    }
}
