package software.amazon.kinesis.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class UserAgentUtils {
    private static final String CONSUMER_ID_PREFIX = "KCL-ConsumerId-";
    private static final String HASH_ALGORITHM = "SHA-256";

    public static String generateConsumerId(String tableArn) {
        if (StringUtils.isEmpty(tableArn)) {
            log.warn("Table ARN is empty, cannot generate consumer ID");
            return null;
        }

        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hashBytes = digest.digest(tableArn.getBytes(StandardCharsets.UTF_8));
            String hash = Base64.getEncoder().encodeToString(hashBytes);
            // Use only a portion of the hash to keep the UserAgent header reasonably sized
            String shortHash = hash.substring(0, Math.min(hash.length(), 16));
            return CONSUMER_ID_PREFIX + shortHash;
        } catch (NoSuchAlgorithmException e) {
            log.error("Failed to generate consumer ID due to hashing algorithm error", e);
            return null;
        }
    }

    public static String getConsumerId(String tableArn) {
        return generateConsumerId(tableArn);
    }
}
