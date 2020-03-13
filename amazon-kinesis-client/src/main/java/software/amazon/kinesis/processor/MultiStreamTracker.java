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
     * Returns the list of stream config, to be processed by the current application.
     *
     * @return List of StreamConfig
     */
    List<StreamConfig> streamConfigList();
}
