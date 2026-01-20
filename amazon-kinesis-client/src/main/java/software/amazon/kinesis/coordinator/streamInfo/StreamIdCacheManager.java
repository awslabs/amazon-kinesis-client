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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

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

    @NonNull
    private final MetricsFactory metricsFactory;

    Queue<String> delayedFetchStreamIdKeys = new ConcurrentLinkedQueue<>();
    final ConcurrentMap<String, CompletableFuture<String>> inFlightRequests = new ConcurrentHashMap<>();
    private final Set<String> inQueueStreamIdKeys = ConcurrentHashMap.newKeySet();

    private static final long STREAM_ID_FETCH_TIMEOUT_MILLIS = 10000L;
    private static final long DELAYED_FETCH_SLEEP_MILLIS = 2000L;

    private static final String METRICS_OPERATION = "StreamIdCacheBackfill";
    private static final String METRIC_SUCCESS = "Success";
    private static final String METRIC_FAILURE = "Failure";
    private static final String METRIC_QUEUE_SIZE = "QueueSize";

    public StreamIdCacheManager(
            ScheduledExecutorService scheduledExecutorService,
            StreamInfoDAO streamInfoDAO,
            Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            StreamIdOnboardingState streamIdOnboardingState,
            boolean isMultiStreamMode,
            @NotNull MetricsFactory metricsFactory) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.streamInfoDAO = streamInfoDAO;
        this.currentStreamConfigMap = currentStreamConfigMap;
        this.streamIdOnboardingState = streamIdOnboardingState;
        this.isMultiStreamMode = isMultiStreamMode;
        this.metricsFactory = metricsFactory;
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
            final String key = streamIdentifier.toString();
            if (inQueueStreamIdKeys.add(key)) {
                delayedFetchStreamIdKeys.add(key);
            }
            return null;
        }
        // streamId is required in StreamIdOnboardingState.ONBOARDED state
        final String streamId = getStreamId(streamIdentifier);
        if (StringUtils.isEmpty(streamId)) {
            final String errorMessage = "Unable to get StreamId for stream identifier: " + streamIdentifier;
            log.error(errorMessage);
            throw new InvalidStateException(errorMessage);
        }
        return streamId;
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
                        processDelayedStreamIdQueue();
                    } catch (Throwable t) {
                        final String message = "Unexpected error in stream ID resolution task. Will retry in {} ms";
                        if (shouldBlockOnStreamId()) {
                            log.error(message, DELAYED_FETCH_SLEEP_MILLIS, t);
                        } else {
                            log.debug(message, DELAYED_FETCH_SLEEP_MILLIS, t);
                        }
                    }
                },
                0,
                DELAYED_FETCH_SLEEP_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private void processDelayedStreamIdQueue() {
        int initialQueueSize = delayedFetchStreamIdKeys.size();
        if (initialQueueSize == 0) {
            log.debug("No delayed fetch stream ID keys found");
            return;
        }

        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, METRICS_OPERATION);
        try {
            int successCount = 0;
            int failureCount = 0;
            String streamIdKey;
            while ((streamIdKey = delayedFetchStreamIdKeys.poll()) != null) {
                inQueueStreamIdKeys.remove(streamIdKey);
                try {
                    final String result = resolveAndFetchStreamId(streamIdKey);
                    if (StringUtils.isNotEmpty(result)) {
                        successCount++;
                    } else {
                        failureCount++;
                    }
                } catch (ProvisionedThroughputException | InvalidStateException | DependencyException e) {
                    failureCount++;
                    if (shouldBlockOnStreamId()) {
                        log.error("Failed to resolve stream ID for key: {}", streamIdKey, e);
                    } else {
                        log.debug(
                                "Failed to resolve stream ID for key: {}. Stream ID may not be supported in this region.",
                                streamIdKey,
                                e);
                    }
                }
            }
            scope.addData(METRIC_QUEUE_SIZE, initialQueueSize, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData(METRIC_SUCCESS, successCount, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData(METRIC_FAILURE, failureCount, StandardUnit.COUNT, MetricsLevel.SUMMARY);

        } finally {
            MetricsUtil.endScope(scope);
        }
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
