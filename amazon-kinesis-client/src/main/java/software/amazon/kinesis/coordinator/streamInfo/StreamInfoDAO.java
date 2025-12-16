package software.amazon.kinesis.coordinator.streamInfo;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

@Slf4j
@KinesisClientInternalApi
public class StreamInfoDAO {

    private final CoordinatorStateDAO coordinatorStateDAO;
    private final KinesisAsyncClient kinesisAsyncClient;

    private static final long INITIAL_BACKOFF_MILLIS = 500;
    private static final long MAX_BACKOFF_MILLIS = 5000;
    private static final int MAX_RETRIES = 5;
    private static final int JITTER_RANGE = 100;
    private static final String ENTITY_TYPE = "STREAM";

    public StreamInfoDAO(CoordinatorStateDAO coordinatorStateDAO, KinesisAsyncClient kinesisAsyncClient) {
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.kinesisAsyncClient = kinesisAsyncClient;
    }

    /**
     * Create a new {@link StreamInfo} if it does not exist.
     * @param streamIdentifier the stream identifier used to create the {@link StreamInfo}
     * @return true if state was streamInfo, false if it already exists
     *
     * @throws DependencyException if DynamoDB put fails in an unexpected way
     * @throws InvalidStateException if Coordinator table does not exist
     * @throws ProvisionedThroughputException if DynamoDB put fails due to lack of capacity
     */
    public boolean createStreamInfo(StreamIdentifier streamIdentifier)
            throws InvalidStateException, DependencyException, ProvisionedThroughputException {
        final String streamId = getStreamId(streamIdentifier);
        log.info("StreamId response from describeStreamSummary call is {} for stream {}", streamId, streamIdentifier);
        if (streamId == null || "null".equals(streamId) || streamId.isEmpty()) {
            log.error("errorring");
            throw new InvalidStateException("Failed to create stream metadata because StreamId response from "
                    + "describeStreamSummary call is null or empty for streamIdentifier " + streamIdentifier);
        }
        final StreamInfo streamInfo = new StreamInfo(streamIdentifier.toString(), streamId, ENTITY_TYPE);
        return coordinatorStateDAO.createCoordinatorStateIfNotExists(streamInfo);
    }

    /**
     * Deletes {@link StreamInfo} if it does exist.
     * @param key the key to delete
     * @return true if state was deleted, false if you cannot be deleted
     *
     * @throws DependencyException if DynamoDB delete fails in an unexpected way
     * @throws InvalidStateException if Coordinator table does not exist
     * @throws ProvisionedThroughputException if DynamoDB delete fails due to lack of capacity
     */
    public boolean deleteStreamInfo(String key)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        return coordinatorStateDAO.deleteCoordinatorState(key);
    }

    /**
     * List all the {@link StreamInfo} from the DDB table synchronously
     *
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if ddb Coordinator does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     *
     * @return list of state
     */
    public List<StreamInfo> listStreamInfo()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final List<CoordinatorState> coordinatorStateList =
                coordinatorStateDAO.listCoordinatorStateByEntityType(ENTITY_TYPE);
        return coordinatorStateList.stream()
                .map(state -> StreamInfo.deserialize(state.getKey(), state.getAttributes()))
                .collect(Collectors.toList());
    }

    /**
     * @param key Get the {@link StreamInfo} for this key.
     *
     * @throws InvalidStateException if ddb table does not exist
     * @throws ProvisionedThroughputException if DynamoDB get fails due to lack of capacity
     * @throws DependencyException if DynamoDB get fails in an unexpected way
     *
     * @return state for the specified key, or null if one doesn't exist
     */
    public StreamInfo getStreamInfo(String key)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        final CoordinatorState coordinatorState = coordinatorStateDAO.getCoordinatorState(key);
        if (coordinatorState == null) {
            log.warn("Could not find StreamInfo for key {}", key);
            return null;
        }
        return StreamInfo.deserialize(coordinatorState.getKey(), coordinatorState.getAttributes());
    }

    /**
     * Get the stream id for the given stream identifier.
     *
     * @param streamIdentifier the stream identifier
     * @return the stream id
     *
     * @throws DependencyException if describeStreamSummary fails in an unexpected way
     */
    private String getStreamId(StreamIdentifier streamIdentifier) throws DependencyException {
        final DescribeStreamSummaryRequest request = DescribeStreamSummaryRequest.builder()
                .streamARN(
                        streamIdentifier.streamArnOptional().isPresent()
                                ? streamIdentifier.streamArnOptional().get().toString()
                                : null)
                .streamName(streamIdentifier.streamName())
                .build();

        int retryCount = 0;
        long backoffTime = INITIAL_BACKOFF_MILLIS;

        while (retryCount < MAX_RETRIES) {
            try {
                final DescribeStreamSummaryResponse response =
                        kinesisAsyncClient.describeStreamSummary(request).get();
                return response.streamDescriptionSummary().streamId();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof LimitExceededException) {
                    if (retryCount == MAX_RETRIES - 1) {
                        throw new DependencyException("Max retries exceeded while describing stream", e);
                    }

                    try {
                        Thread.sleep(backoffTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new DependencyException("Interrupted while backing off", ie);
                    }

                    // Exponential backoff with jitter
                    backoffTime = Math.min(backoffTime * 2, MAX_BACKOFF_MILLIS)
                            + ThreadLocalRandom.current().nextLong(JITTER_RANGE);
                    retryCount++;

                } else {
                    throw new DependencyException("Error describing stream", e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DependencyException("Error describing stream summary", e);
            }
        }
        log.error("Max retries exceeded while describing stream: {}", streamIdentifier);
        return null;
    }
}
