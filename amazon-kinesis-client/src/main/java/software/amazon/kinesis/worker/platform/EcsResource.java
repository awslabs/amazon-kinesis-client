package software.amazon.kinesis.worker.platform;

import java.util.Map;
import java.util.Optional;

import org.jetbrains.annotations.VisibleForTesting;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

import static software.amazon.kinesis.worker.platform.OperatingRangeDataProvider.LINUX_ECS_METADATA_KEY_V4;

/**
 * Provides resource metadata for ECS.
 */
@KinesisClientInternalApi
public class EcsResource implements ResourceMetadataProvider {
    static final String ECS_METADATA_KEY_V3 = "ECS_CONTAINER_METADATA_URI";
    static final String ECS_METADATA_KEY_V4 = "ECS_CONTAINER_METADATA_URI_V4";

    private final Map<String, String> sysEnv;

    @VisibleForTesting
    EcsResource(Map<String, String> sysEnv) {
        this.sysEnv = sysEnv;
    }

    /**
     * Factory method to create an instance of EcsResource.
     *
     * @return an instance of EcsResource
     */
    public static EcsResource create() {
        return new EcsResource(System.getenv());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOnPlatform() {
        return !sysEnv.getOrDefault(ECS_METADATA_KEY_V3, "").isEmpty()
                || !sysEnv.getOrDefault(ECS_METADATA_KEY_V4, "").isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ComputePlatform getPlatform() {
        return ComputePlatform.ECS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<OperatingRangeDataProvider> getOperatingRangeDataProvider() {
        return Optional.of(LINUX_ECS_METADATA_KEY_V4).filter(OperatingRangeDataProvider::isProvider);
    }
}
