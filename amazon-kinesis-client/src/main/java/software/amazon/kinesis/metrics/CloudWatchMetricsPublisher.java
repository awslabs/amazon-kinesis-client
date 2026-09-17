/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.metrics;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.kinesis.retrieval.AWSExceptionManager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Publisher that contains the logic to publish metrics.
 */
@Slf4j
public class CloudWatchMetricsPublisher {
    /**
     * Maximum number of MetricDatums the CloudWatch {@code PutMetricData} API accepts in a single request.
     */
    private static final int MAX_BATCH_SIZE = 1000;

    /**
     * Byte budget for a single {@code PutMetricData} request payload. The CloudWatch service limits each request
     * to 1 MB in size for the HTTP POST request. The 100 KB margin here below 1MB will be
     * the headroom in case the size estimates are off.
     */
    private static final int MAX_REQUEST_PAYLOAD_BYTES = 900_000;

    /**
     * Fixed per-request overhead (request envelope: action, version, and other framing) counted once per
     * {@code PutMetricData} request, before any datum-specific or namespace bytes. Modeled on the CloudWatch
     * agent's {@code overallConstPerRequestSize}. Deliberately generous so the total
     * estimate errs high.
     */
    private static final int PER_REQUEST_OVERHEAD_BYTES = 64;

    /**
     * Fixed per-datum framing overhead (member index, timestamp, and structural punctuation) counted once for
     * every {@link MetricDatum}, on top of its variable-length fields. Deliberately generous so the total
     * estimate errs high.
     */
    private static final int PER_DATUM_OVERHEAD_BYTES = 96;

    /** Per-dimension framing overhead, on top of the UTF-8 lengths of the dimension name and value. */
    private static final int PER_DIMENSION_OVERHEAD_BYTES = 64;

    /**
     * Cost of a single {@code StatisticValues} block (SampleCount, Sum, Minimum, Maximum) including field names
     * and formatted double values. A generous flat estimate; the reference agent uses 246 bytes.
     */
    private static final int STATISTIC_SET_BYTES = 256;

    /**
     * Cost of a single entry in the {@code Values}/{@code Counts} arrays (one value and its paired count),
     * including field names and formatted numeric values. The reference agent uses ~104 bytes per pair.
     */
    private static final int PER_VALUE_COUNT_ENTRY_BYTES = 112;

    /** Cost of the scalar {@code Value} field (field name plus a formatted double). */
    private static final int VALUE_FIELD_BYTES = 48;

    /** Cost of the {@code Unit} field (field name plus the unit token). */
    private static final int UNIT_FIELD_BYTES = 48;

    /** Cost of the {@code StorageResolution} field when high-resolution (1s) metrics are used. */
    private static final int STORAGE_RESOLUTION_FIELD_BYTES = 48;

    private static final int PUT_TIMEOUT_MILLIS = 5000;
    private static final AWSExceptionManager CW_EXCEPTION_MANAGER = new AWSExceptionManager();

    static {
        CW_EXCEPTION_MANAGER.add(CloudWatchException.class, t -> t);
    }

    private final String namespace;

    /**
     * Fixed byte cost present in every {@code PutMetricData} request we build: the request envelope overhead
     * plus the namespace, which is immutable for this publisher. Precomputed once so it is not recalculated for
     * every batch.
     */
    private final int perRequestSeedBytes;

    private final CloudWatchAsyncClient cloudWatchAsyncClient;

    public CloudWatchMetricsPublisher(CloudWatchAsyncClient cloudWatchClient, String namespace) {
        this.cloudWatchAsyncClient = cloudWatchClient;
        this.namespace = namespace;
        this.perRequestSeedBytes = PER_REQUEST_OVERHEAD_BYTES + namespace.getBytes(StandardCharsets.UTF_8).length;
    }

    /**
     * Given a list of MetricDatumWithKey, this method extracts the MetricDatum from each
     * MetricDatumWithKey and publishes those datums. Datums are packed into as few {@code PutMetricData}
     * requests as possible, respecting both {@link #MAX_BATCH_SIZE} and {@link #MAX_REQUEST_PAYLOAD_BYTES}
     * with headroom.
     *
     * @param dataToPublish a list containing all the MetricDatums to publish
     */
    public void publishMetrics(List<MetricDatumWithKey<CloudWatchMetricKey>> dataToPublish) {
        int index = 0;
        final int total = dataToPublish.size();
        while (index < total) {
            final List<MetricDatum> metricData = new ArrayList<>();
            // Seed with the fixed per-request overhead (envelope + namespace), precomputed in the constructor,
            // so the byte budget accounts for bytes present in every request regardless of the datum count.
            int batchBytes = perRequestSeedBytes;
            while (index < total && metricData.size() < MAX_BATCH_SIZE) {
                final MetricDatum datum = dataToPublish.get(index).datum;
                final int datumBytes = estimateDatumBytes(datum);
                // Always include at least one datum per request, even if a single datum somehow exceeds the
                // payload budget, otherwise we would loop forever. Otherwise, stop adding once the budget is hit.
                if (!metricData.isEmpty() && batchBytes + datumBytes > MAX_REQUEST_PAYLOAD_BYTES) {
                    break;
                }
                metricData.add(datum);
                batchBytes += datumBytes;
                index++;
            }

            final int datumCount = metricData.size();
            try {
                final PutMetricDataRequest request = PutMetricDataRequest.builder()
                        .namespace(namespace)
                        .metricData(metricData)
                        .build();
                // This needs to be blocking. Making it asynchronous leads to increased throttling.
                blockingExecute(cloudWatchAsyncClient.putMetricData(request), PUT_TIMEOUT_MILLIS, CW_EXCEPTION_MANAGER);
            } catch (CloudWatchException | TimeoutException e) {
                log.warn("Could not publish {} datums to CloudWatch", datumCount, e);
            } catch (Exception e) {
                log.error("Unknown exception while publishing {} datums to CloudWatch", datumCount, e);
            }
        }
    }

    /**
     * Estimates the serialized size, in bytes, that a {@link MetricDatum} contributes to a {@code PutMetricData}
     * request payload. Used only to decide when to split a request below the 1 MB service limit.
     * Err on the higher side of the estimates than being accurate.
     */
    static int estimateDatumBytes(final MetricDatum datum) {
        int bytes = PER_DATUM_OVERHEAD_BYTES;

        if (datum.metricName() != null) {
            bytes += utf8Length(datum.metricName());
        }

        if (datum.hasDimensions()) {
            for (final Dimension dimension : datum.dimensions()) {
                bytes += PER_DIMENSION_OVERHEAD_BYTES;
                if (dimension.name() != null) {
                    bytes += utf8Length(dimension.name());
                }
                if (dimension.value() != null) {
                    bytes += utf8Length(dimension.value());
                }
            }
        }

        // A datum carries EITHER a scalar Value, OR a StatisticValues set, OR paired Values/Counts arrays.
        if (datum.statisticValues() != null) {
            bytes += STATISTIC_SET_BYTES;
        }
        if (datum.hasValues() && !datum.values().isEmpty()) {
            // Values and Counts are parallel arrays; charge per entry (covers both a value and its count).
            bytes += datum.values().size() * PER_VALUE_COUNT_ENTRY_BYTES;
        }
        if (datum.value() != null) {
            bytes += VALUE_FIELD_BYTES;
        }

        if (datum.unit() != null) {
            bytes += UNIT_FIELD_BYTES;
        }
        if (datum.storageResolution() != null) {
            bytes += STORAGE_RESOLUTION_FIELD_BYTES;
        }

        return bytes;
    }

    private static int utf8Length(final String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    private static <T> void blockingExecute(
            CompletableFuture<T> future, long timeOutMillis, AWSExceptionManager exceptionManager)
            throws TimeoutException {
        try {
            future.get(timeOutMillis, MILLISECONDS);
        } catch (ExecutionException e) {
            throw exceptionManager.apply(e.getCause());
        } catch (InterruptedException e) {
            log.info("Thread interrupted.");
        }
    }
}
