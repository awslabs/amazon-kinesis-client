package software.amazon.kinesis.coordinator;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.streamInfo.StreamIdOnboardingState;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfo;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoDAO;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoMode;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;

@Data
@Accessors(fluent = true)
@Slf4j
@RequiredArgsConstructor
@KinesisClientInternalApi
public class StreamInfoManager {
    @NonNull
    private final ScheduledExecutorService scheduledExecutorService;

    @NonNull
    private final Map<StreamIdentifier, StreamConfig> currentStreamConfigMap;

    @NonNull
    private final StreamInfoDAO streamInfoDAO;

    @NonNull
    private final MetricsFactory metricsFactory;

    private final boolean isMultiStreamMode;

    private final long streamInfoBackfillIntervalMillis;

    private final StreamInfoMode streamInfoMode;

    private final StreamIdOnboardingState streamIdOnboardingState;

    private ScheduledFuture<?> scheduledFuture;
    private boolean isRunning;

    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MILLIS = 1000;
    private static final long DEFAULT_AWAIT_TERMINATION_TIMEOUT_MILLIS = 10000L;

    public synchronized void start() {
        if (!needStreamInfo()) {
            return;
        }
        if (!isRunning) {
            scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(
                    this::performBackfill, 0L, streamInfoBackfillIntervalMillis, TimeUnit.MILLISECONDS);
            log.info("Started StreamInfoManager");
            isRunning = true;
        }
    }

    public synchronized void stop(boolean isShutdown) {
        if (isRunning) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                log.info("Cancelled scheduled stream metadata back-fill task");
            }
            if (isShutdown) {
                scheduledExecutorService.shutdown();
                try {
                    if (!scheduledExecutorService.awaitTermination(
                            DEFAULT_AWAIT_TERMINATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                        scheduledExecutorService.shutdownNow();
                        // Wait a while for tasks to respond to being cancelled
                        if (!scheduledExecutorService.awaitTermination(
                                DEFAULT_AWAIT_TERMINATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                            log.error("Executor service didn't terminate");
                        }
                    }
                } catch (InterruptedException e) {
                    // (Re-)Cancel if current thread also interrupted
                    scheduledExecutorService.shutdownNow();
                    // Preserve interrupt status
                    Thread.currentThread().interrupt();
                }
            }
            isRunning = false;
        }
    }

    /**
     * Creates new streamInfo
     * @param streamIdentifier streamIdentifier to create streamInfo for
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    public void createStreamInfo(StreamIdentifier streamIdentifier)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        if (!needStreamInfo()) {
            return;
        }
        streamInfoDAO.createStreamInfo(streamIdentifier);
    }

    /**
     * Deletes {@link StreamInfo} if it does exist.
     * @param streamIdentifier streamIdentifier to create streamInfo for
     * @return true if state was deleted, false if you cannot be deleted
     *
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    public void deleteStreamInfo(StreamIdentifier streamIdentifier)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        if (!needStreamInfo()) {
            return;
        }
        streamInfoDAO.deleteStreamInfo(streamIdentifier.serialize());
    }

    /**
     * This method will backfill stream info for all the streams that are currently being tracked.
     * It will retry backfill of failed streams after a delay of {@link #streamInfoBackfillIntervalMillis}
     * It does dot throw exception, it will just log and retry backfill of failed streams info again
     */
    private void performBackfill() {
        Set<StreamIdentifier> successfulBackfillStreams = new HashSet<>();
        final Set<StreamIdentifier> streamConfigKeys = currentStreamConfigMap.keySet();
        try {
            log.debug("Running stream metadata backfill task..");
            final Set<String> streamInfos = streamInfoDAO.listStreamInfo().stream()
                    .map(StreamInfo::getKey)
                    .collect(Collectors.toSet());

            for (StreamIdentifier streamIdentifier : streamConfigKeys) {
                final StreamConfig streamConfig = currentStreamConfigMap.get(streamIdentifier);
                if (streamConfig == null) {
                    successfulBackfillStreams.add(streamIdentifier);
                    log.debug("Skipping stream metadata sync task for {} as stream is purged", streamIdentifier);
                    continue;
                }
                try {
                    if (!streamInfos.contains(streamIdentifier.serialize())) {
                        if (streamInfoDAO.createStreamInfo(streamIdentifier)) {
                            successfulBackfillStreams.add(streamIdentifier);
                        }
                    } else {
                        successfulBackfillStreams.add(streamIdentifier);
                        log.debug("Stream metadata already exists for streamIdentifier: {}", streamIdentifier);
                    }
                } catch (Exception e) {
                    final String errorMessage =
                            "Caught exception while syncing streamId " + streamIdentifier + ". Will retry in next run";
                    if (streamIdOnboardingState == StreamIdOnboardingState.ONBOARDED) {
                        log.error(errorMessage, e);
                    } else {
                        log.debug(errorMessage, e);
                    }
                }
            }
        } catch (Exception e) {
            final String errorMessage = "Caught exception while syncing streamId.";
            if (streamIdOnboardingState == StreamIdOnboardingState.ONBOARDED) {
                log.error(errorMessage, e);
            } else {
                log.debug(errorMessage, e);
            }
        }

        Queue<StreamIdentifier> failedBackfillStreams = null;
        try {
            failedBackfillStreams = new LinkedList<>(streamConfigKeys);
            failedBackfillStreams.removeAll(successfulBackfillStreams);

            retryFailedBackfill(failedBackfillStreams);
        } catch (Exception e) {
            log.error(
                    "Error while retrying stream info backfill for streams: {}. Will retry later.",
                    failedBackfillStreams);
        }
    }

    /**
     * Retries backfill of failed streams info
     * Does not throw exception as we will retry backfill of failed streams again
     * @param failedBackfillStreams list of streamIdentifier for which backfill failed
     */
    private void retryFailedBackfill(Queue<StreamIdentifier> failedBackfillStreams) {
        while (!failedBackfillStreams.isEmpty()) {
            final StreamIdentifier streamIdentifier = failedBackfillStreams.poll();
            try {
                if (currentStreamConfigMap.containsKey(streamIdentifier)) {
                    createStreamInfoWithRetry(streamIdentifier);
                }
            } catch (Exception e) {
                log.error("Caught exception while syncing streamId {}. Will retry in next run", streamIdentifier, e);
            }
        }
    }

    /**
     * Creates stream info for given stream identifier with retry.
     * @param streamIdentifier stream identifier to create stream info for
     *
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    private void createStreamInfoWithRetry(StreamIdentifier streamIdentifier)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        int retryCount = 0;
        long backoffTime = INITIAL_BACKOFF_MILLIS;

        while (retryCount < MAX_RETRIES) {
            if (streamInfoDAO.createStreamInfo(streamIdentifier)) {
                log.debug("Successfully created stream metadata for streamIdentifier: {}", streamIdentifier);
                return;
            }
            retryCount++;
            if (retryCount >= MAX_RETRIES) {
                log.error(
                        "Stream metadata creation failed after {} attempts for streamIdentifier: {}",
                        MAX_RETRIES,
                        streamIdentifier);
                return;
            }

            log.warn(
                    "Stream metadata creation failed for streamIdentifier: {}, retrying (attempt {}/{})",
                    streamIdentifier,
                    retryCount,
                    MAX_RETRIES);

            try {
                Thread.sleep(backoffTime);
                backoffTime *= 2;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting to retry stream metadata creation");
                return;
            }
        }
    }

    private boolean needStreamInfo() {
        return streamInfoMode != StreamInfoMode.DISABLED;
    }
}
