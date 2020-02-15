package software.amazon.kinesis.processor;

import software.amazon.kinesis.common.InitialPositionInStreamExtended;

import java.util.List;

/**
 * Interface for stream trackers. This is useful for KCL Workers that need
 * to consume data from multiple streams.
 */
public interface MultiStreamTracker {

    /**
     * Returns the list of streams that the Worker should consume data from.
     *
     * @return List of stream names
     */
    List<String> listStreamsToProcess();

    /**
     * Returns the initial position in stream to read from, for the given stream.
     * @param streamName
     * @return Initial position to read from, for the given stream
     */
    InitialPositionInStreamExtended initialPositionInStreamExtended(String streamName);

}
