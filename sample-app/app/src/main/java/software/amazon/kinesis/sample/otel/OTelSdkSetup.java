package software.amazon.kinesis.sample.otel;

import java.time.Duration;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures the OpenTelemetry SDK with a SigV4-authenticated OTLP exporter
 * that sends metrics directly to the CloudWatch OTLP endpoint.
 *
 * <p>No ADOT. Uses a custom MetricExporter backed by the AWS SDK's Aws4Signer
 * and HTTP client to sign OTLP requests for CloudWatch.
 */
public class OTelSdkSetup {
    private static final Logger log = LoggerFactory.getLogger(OTelSdkSetup.class);

    public static OpenTelemetrySdk initialize(SampleAppConfig config) {
        String endpoint = "https://monitoring." + config.getRegion() + ".amazonaws.com/v1/metrics";
        log.info("Initializing OTel SDK with SigV4 OTLP exporter: endpoint={}, interval={}ms",
                endpoint, config.getExportIntervalMillis());

        Resource resource = Resource.getDefault().merge(
                Resource.create(Attributes.builder()
                        .put(ResourceAttributes.SERVICE_NAME, config.getApplicationName())
                        .put(ResourceAttributes.SERVICE_VERSION, "1.0.0")
                        .put(ResourceAttributes.CLOUD_PROVIDER, "aws")
                        .put(ResourceAttributes.CLOUD_REGION, config.getRegion())
                        .build()));

        // Custom exporter that signs OTLP requests with SigV4
        MetricExporter exporter = new SigV4OtlpMetricExporter(config.getRegion());

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(
                        PeriodicMetricReader.builder(exporter)
                                .setInterval(Duration.ofMillis(config.getExportIntervalMillis()))
                                .build())
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();

        try {
            GlobalOpenTelemetry.set(sdk);
        } catch (IllegalStateException e) {
            log.warn("GlobalOpenTelemetry already set", e);
        }

        log.info("OTel SDK initialized successfully");
        return sdk;
    }

    public static void shutdown(OpenTelemetrySdk sdk) {
        if (sdk != null) {
            log.info("Shutting down OTel SDK, flushing buffered metrics...");
            sdk.getSdkMeterProvider().shutdown();
        }
    }
}
