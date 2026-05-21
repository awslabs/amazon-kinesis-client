package software.amazon.kinesis.sample.otel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Collection;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

/**
 * OTel MetricExporter that sends metrics to CloudWatch's OTLP endpoint with SigV4 auth.
 *
 * <p>Strategy: delegates to OtlpHttpMetricExporter for serialization by capturing
 * the protobuf bytes, then sends them via the AWS SDK HTTP client with SigV4 signing.
 *
 * <p>Since OTel doesn't expose a public serialization API, we use a workaround:
 * serialize by creating a temporary in-memory OTLP exporter that writes to a
 * capturing endpoint, then sign and send the captured bytes.
 *
 * <p>Actually, the simplest working approach: use OtlpHttpMetricExporter pointed at
 * a local no-op server to get serialization, OR just use the JSON format which we
 * can construct manually from MetricData.
 *
 * <p>FINAL APPROACH: Use the OTel SDK's built-in OTLP JSON serialization via
 * the exporter's internal marshaler accessed reflectively, OR just send JSON
 * format which CloudWatch OTLP also accepts.
 */
public class SigV4OtlpMetricExporter implements MetricExporter {
    private static final Logger log = LoggerFactory.getLogger(SigV4OtlpMetricExporter.class);

    private final URI endpoint;
    private final Region region;
    private final AwsCredentialsProvider credentialsProvider;
    private final Aws4Signer signer;
    private final SdkHttpClient httpClient;
    // Delegate exporter for serialization — we intercept at the network layer
    private final OtlpHttpMetricExporter delegate;

    public SigV4OtlpMetricExporter(String regionStr) {
        this.endpoint = URI.create("https://monitoring." + regionStr + ".amazonaws.com/v1/metrics");
        this.region = Region.of(regionStr);
        this.credentialsProvider = DefaultCredentialsProvider.create();
        this.signer = Aws4Signer.create();
        this.httpClient = ApacheHttpClient.create();
        // We won't actually use the delegate to send — just for temporality config
        this.delegate = OtlpHttpMetricExporter.builder()
                .setEndpoint(this.endpoint.toString())
                .build();
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        if (metrics.isEmpty()) {
            return CompletableResultCode.ofSuccess();
        }

        try {
            // Serialize to OTLP JSON (CloudWatch accepts both protobuf and JSON)
            String json = OtlpJsonSerializer.serialize(metrics);
            byte[] body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            // Build HTTP request
            SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                    .uri(endpoint)
                    .method(SdkHttpMethod.POST)
                    .putHeader("Content-Type", "application/json")
                    .contentStreamProvider(() -> new ByteArrayInputStream(body));

            // Sign with SigV4 (service name: "monitoring")
            Aws4SignerParams signerParams = Aws4SignerParams.builder()
                    .awsCredentials(credentialsProvider.resolveCredentials())
                    .signingName("monitoring")
                    .signingRegion(region)
                    .build();

            SdkHttpFullRequest signedRequest = signer.sign(requestBuilder.build(), signerParams);

            // Execute
            HttpExecuteResponse response = httpClient.prepareRequest(
                    HttpExecuteRequest.builder()
                            .request(signedRequest)
                            .contentStreamProvider(signedRequest.contentStreamProvider().orElse(null))
                            .build())
                    .call();

            int statusCode = response.httpResponse().statusCode();
            if (statusCode >= 200 && statusCode < 300) {
                log.debug("Exported {} metrics to CloudWatch OTLP (HTTP {})", metrics.size(), statusCode);
                return CompletableResultCode.ofSuccess();
            } else {
                log.warn("CloudWatch OTLP returned HTTP {}, {} metrics may be lost", statusCode, metrics.size());
                return CompletableResultCode.ofFailure();
            }
        } catch (Exception e) {
            log.warn("Failed to export metrics to CloudWatch OTLP: {}", e.getMessage(), e);
            return CompletableResultCode.ofFailure();
        }
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        httpClient.close();
        delegate.shutdown();
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return AggregationTemporality.CUMULATIVE;
    }
}
