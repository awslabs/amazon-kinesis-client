package software.amazon.kinesis.sample.otel;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.resources.Resource;

/**
 * Serializes OTel MetricData to OTLP JSON format for the CloudWatch OTLP endpoint.
 * Implements the OTLP/HTTP JSON encoding per the OpenTelemetry specification.
 */
public class OtlpJsonSerializer {

    public static String serialize(Collection<MetricData> metrics) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resourceMetrics\":[");

        // Group by resource
        Map<Resource, java.util.List<MetricData>> byResource = metrics.stream()
                .collect(Collectors.groupingBy(MetricData::getResource));

        boolean firstResource = true;
        for (var entry : byResource.entrySet()) {
            if (!firstResource) sb.append(",");
            firstResource = false;

            sb.append("{\"resource\":{\"attributes\":[");
            appendAttributes(sb, entry.getKey().getAttributes());
            sb.append("]},\"scopeMetrics\":[{\"scope\":{\"name\":\"software.amazon.kinesis\"},\"metrics\":[");

            boolean firstMetric = true;
            for (MetricData metric : entry.getValue()) {
                if (!firstMetric) sb.append(",");
                firstMetric = false;
                appendMetric(sb, metric);
            }

            sb.append("]}]}");
        }

        sb.append("]}");
        return sb.toString();
    }

    private static void appendMetric(StringBuilder sb, MetricData metric) {
        sb.append("{\"name\":\"").append(escapeJson(metric.getName())).append("\"");
        if (!metric.getUnit().isEmpty()) {
            sb.append(",\"unit\":\"").append(escapeJson(metric.getUnit())).append("\"");
        }

        switch (metric.getType()) {
            case LONG_SUM:
                sb.append(",\"sum\":{\"dataPoints\":[");
                boolean first = true;
                for (LongPointData point : metric.getLongSumData().getPoints()) {
                    if (!first) sb.append(",");
                    first = false;
                    appendLongDataPoint(sb, point);
                }
                sb.append("],\"aggregationTemporality\":2,\"isMonotonic\":true}");
                break;
            case DOUBLE_SUM:
                sb.append(",\"sum\":{\"dataPoints\":[");
                first = true;
                for (DoublePointData point : metric.getDoubleSumData().getPoints()) {
                    if (!first) sb.append(",");
                    first = false;
                    appendDoubleDataPoint(sb, point);
                }
                sb.append("],\"aggregationTemporality\":2,\"isMonotonic\":true}");
                break;
            case HISTOGRAM:
                sb.append(",\"histogram\":{\"dataPoints\":[");
                first = true;
                for (HistogramPointData point : metric.getHistogramData().getPoints()) {
                    if (!first) sb.append(",");
                    first = false;
                    appendHistogramDataPoint(sb, point);
                }
                sb.append("],\"aggregationTemporality\":2}");
                break;
            case LONG_GAUGE:
                sb.append(",\"gauge\":{\"dataPoints\":[");
                first = true;
                for (LongPointData point : metric.getLongGaugeData().getPoints()) {
                    if (!first) sb.append(",");
                    first = false;
                    appendLongDataPoint(sb, point);
                }
                sb.append("]}");
                break;
            case DOUBLE_GAUGE:
                sb.append(",\"gauge\":{\"dataPoints\":[");
                first = true;
                for (DoublePointData point : metric.getDoubleGaugeData().getPoints()) {
                    if (!first) sb.append(",");
                    first = false;
                    appendDoubleDataPoint(sb, point);
                }
                sb.append("]}");
                break;
            default:
                break;
        }
        sb.append("}");
    }

    private static void appendLongDataPoint(StringBuilder sb, LongPointData point) {
        sb.append("{\"timeUnixNano\":\"").append(point.getEpochNanos()).append("\"");
        sb.append(",\"startTimeUnixNano\":\"").append(point.getStartEpochNanos()).append("\"");
        sb.append(",\"asInt\":\"").append(point.getValue()).append("\"");
        sb.append(",\"attributes\":[");
        appendAttributes(sb, point.getAttributes());
        sb.append("]}");
    }

    private static void appendDoubleDataPoint(StringBuilder sb, DoublePointData point) {
        sb.append("{\"timeUnixNano\":\"").append(point.getEpochNanos()).append("\"");
        sb.append(",\"startTimeUnixNano\":\"").append(point.getStartEpochNanos()).append("\"");
        sb.append(",\"asDouble\":").append(point.getValue());
        sb.append(",\"attributes\":[");
        appendAttributes(sb, point.getAttributes());
        sb.append("]}");
    }

    private static void appendHistogramDataPoint(StringBuilder sb, HistogramPointData point) {
        sb.append("{\"timeUnixNano\":\"").append(point.getEpochNanos()).append("\"");
        sb.append(",\"startTimeUnixNano\":\"").append(point.getStartEpochNanos()).append("\"");
        sb.append(",\"count\":\"").append(point.getCount()).append("\"");
        sb.append(",\"sum\":").append(point.getSum());
        if (point.hasMin()) sb.append(",\"min\":").append(point.getMin());
        if (point.hasMax()) sb.append(",\"max\":").append(point.getMax());
        sb.append(",\"bucketCounts\":[");
        var counts = point.getCounts();
        for (int i = 0; i < counts.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(counts.get(i)).append("\"");
        }
        sb.append("],\"explicitBounds\":[");
        var bounds = point.getBoundaries();
        for (int i = 0; i < bounds.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(bounds.get(i));
        }
        sb.append("],\"attributes\":[");
        appendAttributes(sb, point.getAttributes());
        sb.append("]}");
    }

    private static void appendAttributes(StringBuilder sb, Attributes attributes) {
        boolean first = true;
        for (var entry : attributes.asMap().entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("{\"key\":\"").append(escapeJson(entry.getKey().getKey())).append("\"");
            sb.append(",\"value\":{\"stringValue\":\"").append(escapeJson(String.valueOf(entry.getValue()))).append("\"}}");
        }
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
