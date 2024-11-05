package software.amazon.kinesis.worker.platform;

import java.util.Optional;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Interface for providing resource metadata for worker.
 */
@KinesisClientInternalApi
public interface ResourceMetadataProvider {
    /**
     * Enum representing the different compute platforms.
     */
    enum ComputePlatform {
        EC2,
        ECS,
        EKS,
        UNKNOWN
    }

    /**
     * Check if the worker is running on the specific platform.
     *
     * @return true if the worker is running on the specific platform, false otherwise.
     */
    boolean isOnPlatform();

    /**
     * Get the name of the compute platform.
     *
     * @return the platform represent by the class.
     */
    ComputePlatform getPlatform();

    /**
     * Get the operating range data provider.
     *
     * @return the operating range data provider.
     */
    Optional<OperatingRangeDataProvider> getOperatingRangeDataProvider();
}
