package software.amazon.kinesis.processor;

import software.amazon.kinesis.common.StreamConfig;

import java.util.List;
import java.util.Map;

/**
 * Interface for stream trackers. This is useful for KCL Workers that need
 * to consume data from multiple streams.
 */
public interface MultiStreamTracker {

    /**
     * Returns the map of streams and its associated stream specific config.
     *
     * @return List of stream names
     */
    List<StreamConfig> streamConfigList();
}
