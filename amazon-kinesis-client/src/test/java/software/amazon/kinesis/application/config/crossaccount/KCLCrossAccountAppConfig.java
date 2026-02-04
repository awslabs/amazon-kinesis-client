package software.amazon.kinesis.application.config.crossaccount;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.kinesis.application.config.KCLAppConfig;

/**
 * Config class to configure cross account integration tests.
 */
public abstract class KCLCrossAccountAppConfig extends KCLAppConfig {

    @Override
    public boolean isCrossAccount() {
        return true;
    }

    @Override
    public AwsCredentialsProvider getCrossAccountCredentialsProvider() {
        final String awsCrossAccountProfile = System.getProperty(KCLAppConfig.CROSS_ACCOUNT_PROFILE_PROPERTY);
        return (awsCrossAccountProfile != null)
                ? ProfileCredentialsProvider.builder()
                        .profileName(awsCrossAccountProfile)
                        .build()
                : null;
    }
}
