package software.amazon.kinesis.multilang.auth;

import java.net.URI;
import java.net.URISyntaxException;

import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class StsAssumeRoleCredentialsProviderFactory {

    public static StsAssumeRoleCredentialsProvider createProvider(StsAssumeRoleCredentialsProviderConfig config) {
        StsClientBuilder stsClientBuilder = StsClient.builder();

        if (config.getRegion() != null) {
            stsClientBuilder.region(config.getRegion());
        }

        if (config.getServiceEndpoint() != null) {
            try {
                stsClientBuilder.endpointOverride(new URI(config.getServiceEndpoint()));
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid service endpoint: " + config.getServiceEndpoint(), e);
            }
        }

        StsClient stsClient = stsClientBuilder.build();

        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                .roleArn(config.getRoleArn())
                .roleSessionName(config.getRoleSessionName())
                .build();

        return StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(assumeRoleRequest)
                .stsClient(stsClient)
                .build();
    }
}
