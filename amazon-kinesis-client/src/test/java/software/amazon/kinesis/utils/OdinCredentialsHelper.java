package software.amazon.kinesis.utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.io.CharStreams;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Helper class to hold odin credentials because odin is not available externally and this package doesn't use brazil.
 */
@Slf4j
public class OdinCredentialsHelper {

    private final static String PRINCIPAL = "Principal";
    private final static String CREDENTIAL = "Credential";
    private final static String ODIN_COMMAND = "/apollo/env/envImprovement/bin/odin-get -t";

    private static String getMaterial(String materialName, String materialType) throws IOException {
        final InputStream inputStream = Runtime.getRuntime().exec(String.format("%s %s %s", ODIN_COMMAND, materialType, materialName)).getInputStream();
        return CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).trim();
    }
    private static String getPrincipal(String materialName) throws IOException {
        return getMaterial(materialName, PRINCIPAL);
    }

    private static String getCredential(String materialName) throws IOException {
        return getMaterial(materialName, CREDENTIAL);
    }

    /**
     * Helper method to pull credentials from odin for testing for AWS SDK sync clients (1.x).
     *
     * @param materialName name of the material set to fetch.
     * @return access/secret key pair from Odin if specified for testing.
     * @throws IOException
     */
    public static AWSCredentialsProvider getSyncAwsCredentialsFromMaterialSet(String materialName) throws IOException {
        if (materialName == null) {
            log.debug("No material name found.");
            return null;
        }

        log.debug("Fetching credentials for material - {}.", materialName);

        final String principal = getPrincipal(materialName);
        final String credential = getCredential(materialName);

        final AWSCredentialsProvider awsCredentialsProvider = new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new AWSCredentials() {
                    @Override
                    public String getAWSAccessKeyId() {
                        return principal;
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return credential;
                    }
                };
            }
            @Override
            public void refresh() {
            }
        };

        log.debug("Successfully retrieved credentials from odin. Access key - {}.", principal);

        return awsCredentialsProvider;
    }

    /**
     * Helper method to pull credentials from odin for testing for AWS SDK async clients (2.x).
     *
     * @param materialName name of the material set to fetch.
     * @return access/secret key pair from Odin if specified for testing.
     * @throws IOException
     */
    public static AwsCredentialsProvider getAsyncAwsCredentialsFromMaterialSet(String materialName) throws IOException {
        if (materialName == null) {
            log.debug("No material name found.");
            return null;
        }

        log.debug("Fetching credentials for material - {}.", materialName);

        final String principal = getPrincipal(materialName);
        final String credential = getCredential(materialName);

        final AwsCredentialsProvider awsCredentialsProvider = () -> new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return principal;
            }

            @Override
            public String secretAccessKey() {
                return credential;
            }
        };

        log.debug("Successfully retrieved credentials from odin. Access key - {}.", principal);

        return awsCredentialsProvider;
    }
}
