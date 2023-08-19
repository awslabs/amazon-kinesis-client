package software.amazon.kinesis.coordinator;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

import software.amazon.kinesis.common.StreamIdentifier;

/**
 * This class is used for storing in-memory set of streams which are no longer existing (deleted) and needs to be
 * cleaned up from KCL's in memory state.
 */
@Slf4j
public class DeletedStreamListProvider {

    private final Set<StreamIdentifier> deletedStreams;

    public DeletedStreamListProvider() {
        deletedStreams = ConcurrentHashMap.newKeySet();
    }

    public void add(StreamIdentifier streamIdentifier) {
        log.info("Added {}", streamIdentifier);
        deletedStreams.add(streamIdentifier);
    }

    /**
     * Method returns and empties the current set of streams
     * @return set of deleted Streams
     */
    public Set<StreamIdentifier> purgeAllDeletedStream() {
        final Set<StreamIdentifier> response = new HashSet<>(deletedStreams);
        deletedStreams.removeAll(response);
        return response;
    }
}
