package software.amazon.kinesis.coordinator.streamInfo;

import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

@Slf4j
@AllArgsConstructor
@KinesisClientInternalApi
public class StreamIdCacheManager {
    final ConcurrentMap<String, String> cache = new ConcurrentHashMap<>(); // streamIdKey: streamId
    private final StreamInfoDAO streamInfoDAO;
    private final Map<StreamIdentifier, StreamConfig> currentStreamConfigMap;
    private final StreamIdOnboardingState streamIdOnboardingState;
    private final boolean isMultiStreamMode;
    private final ScheduledExecutorService scheduledExecutorService;

    Queue<String> delayedFetchStreamIdKeys = new ConcurrentLinkedQueue<>();
    final ConcurrentMap<String, CompletableFuture<String>> inFlightRequests = new ConcurrentHashMap<>();
    private static final long STREAM_ID_FETCH_TIMEOUT_MILLIS = 10000L;
    private static final long DELAYED_FETCH_SLEEP_MILLIS = 2000L;

    public StreamIdCacheManager(
            ScheduledExecutorService scheduledExecutorService,
            StreamInfoDAO streamInfoDAO,
            Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            StreamIdOnboardingState streamIdOnboardingState,
            boolean isMultiStreamMode) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.streamInfoDAO = streamInfoDAO;
        this.currentStreamConfigMap = currentStreamConfigMap;
        this.streamIdOnboardingState = streamIdOnboardingState;
        this.isMultiStreamMode = isMultiStreamMode;
        runDelayedStreamIdFetchTask();
    }

    public Set<String> getAllCachedStreamIdKey() {
        return cache.keySet();
    }

    /**
     * Get a value from streamIdCache
     * @param streamIdentifier name of the stream
     * @return the stream ID or null or throws exception if stream ID is not found in ONBOARDED mode
     */
    public String get(StreamIdentifier streamIdentifier) throws InvalidStateException {
        if (streamIdentifier == null) {
            streamIdentifier = getStreamIdentifier();
        }
        final String cachedStreamId = cache.get(streamIdentifier.toString());
        if (StringUtils.isNotEmpty(cachedStreamId)) {
            return cachedStreamId;
        }
        if (!shouldBlockOnStreamId()) {
            delayedFetchStreamIdKeys.add(streamIdentifier.toString());
            return null;
        }
        return getStreamId(streamIdentifier);
    }

    /**
     * Gets streamIdentifier from currentStreamConfigMap in single stream kcl mode
     * @return StreamIdentifier
     * @throws IllegalArgumentException
     */
    @NotNull
    private StreamIdentifier getStreamIdentifier() throws IllegalArgumentException {
        if (isMultiStreamMode) {
            throw new IllegalArgumentException("Cannot get streamIdentifier in multi stream mode");
        }
        // single stream mode
        final Optional<StreamIdentifier> currentStreamIdentifier =
                currentStreamConfigMap.keySet().stream().findFirst();
        if (!currentStreamIdentifier.isPresent()) {
            throw new IllegalArgumentException("Could not get stream identifier from currentStreamConfigMap");
        }
        return currentStreamIdentifier.get();
    }

    /**
     * Gets the streamId. Makes sure only one call is in flight for the same streamIdKey
     * @param streamIdentifier
     * @return StreamId for the given streamIdKey or throws exception
     */
    private String getStreamId(StreamIdentifier streamIdentifier) throws IllegalArgumentException {
        if (streamIdentifier == null) {
            streamIdentifier = getStreamIdentifier();
        }
        final String streamIdKey = streamIdentifier.toString();
        String cachedStreamId = cache.get(streamIdKey);
        if (StringUtils.isNotEmpty(cachedStreamId)) {
            return cachedStreamId;
        }
        CompletableFuture<String> future = inFlightRequests.computeIfAbsent(
                streamIdKey,
                k -> CompletableFuture.supplyAsync(() -> {
                    try {
                        return resolveAndFetchStreamId(streamIdKey);
                    } catch (Exception e) {
                        log.error("Error fetching stream ID for {}", streamIdKey, e);
                        throw new CompletionException(e);
                    } finally {
                        inFlightRequests.remove(streamIdKey);
                    }
                }));

        try {
            return future.get(STREAM_ID_FETCH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while fetching stream ID", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error fetching stream ID", e);
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    "Timed out waiting for stream ID after " + STREAM_ID_FETCH_TIMEOUT_MILLIS + "ms", e);
        }
    }

    /**
     * Resolves the stream ID for the given StreamIdentifier and adds it to the cache.
     * @param streamIdentifier streamIdentifier whose StreamId needs to be resolved
     */
    public void resolveStreamId(StreamIdentifier streamIdentifier) {
        if (streamIdOnboardingState == StreamIdOnboardingState.NOT_ONBOARDED) {
            return;
        }
        getStreamId(streamIdentifier);
    }

    public void removeStreamId(String streamIdKey) {
        cache.remove(streamIdKey);
    }

    /**
     * Fetched streamInfo from dynamoDB and adds it to the cache
     * @param streamIdKey name of the stream in single stream mode or the stream identifier string in multi stream mode
     * @return StreamId for the given streamIdKey or null
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    String resolveAndFetchStreamId(String streamIdKey)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        if (!cache.containsKey(streamIdKey)) {
            StreamInfo streamInfo = streamInfoDAO.getStreamInfo(streamIdKey);
            String streamId = null;
            if (streamInfo != null && streamInfo.getStreamId() != null) {
                streamId = streamInfo.getStreamId();
                cache.put(streamIdKey, streamId);
                return streamId;
            }
            if (streamIdOnboardingState == StreamIdOnboardingState.ONBOARDED) {
                throw new RuntimeException("StreamId not found for " + streamIdKey);
            }
            return streamId;
        }
        return cache.get(streamIdKey);
    }

    /**
     * Runs a task to process delayed fetch streamId requests
     * This task will run in a single thread and will process all the requests in the queue
     * This method does not throw exception since it is resolving streamId in non-blocking mode
     * The caller which needs streamId will retry to resolve streamId
     */
    private void runDelayedStreamIdFetchTask() {
        scheduledExecutorService.scheduleWithFixedDelay(
                () -> {
                    try {
                        while (!delayedFetchStreamIdKeys.isEmpty()) {
                            resolveAndFetchStreamId(delayedFetchStreamIdKeys.poll());
                        }
                    } catch (Exception e) {
                        final String errorMessage = "Error processing delayed stream ID fetch";
                        if (shouldBlockOnStreamId()) {
                            log.error(errorMessage, e);
                        } else {
                            log.debug(errorMessage, e);
                        }
                    }
                },
                0,
                DELAYED_FETCH_SLEEP_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private boolean shouldBlockOnStreamId() {
        return streamIdOnboardingState == StreamIdOnboardingState.ONBOARDED;
    }

    public void stop() {
        if (!scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdownNow();
        }
    }
}
