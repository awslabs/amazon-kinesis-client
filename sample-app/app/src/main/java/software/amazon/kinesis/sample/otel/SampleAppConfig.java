package software.amazon.kinesis.sample.otel;

/**
 * Configuration loaded from environment variables.
 */
public class SampleAppConfig {
    private final String streamName;
    private final String applicationName;
    private final String region;
    private final String otlpEndpoint;
    private final long exportIntervalMillis;
    private final int producerRecordsPerSecond;
    private final int producerDurationSeconds;

    private SampleAppConfig(String streamName, String applicationName, String region,
                            String otlpEndpoint, long exportIntervalMillis,
                            int producerRecordsPerSecond, int producerDurationSeconds) {
        this.streamName = streamName;
        this.applicationName = applicationName;
        this.region = region;
        this.otlpEndpoint = otlpEndpoint;
        this.exportIntervalMillis = exportIntervalMillis;
        this.producerRecordsPerSecond = producerRecordsPerSecond;
        this.producerDurationSeconds = producerDurationSeconds;
    }

    public static SampleAppConfig fromEnvironment() {
        String region = env("AWS_REGION", "us-west-2");
        return new SampleAppConfig(
                env("STREAM_NAME", "kcl-otel-sample-stream"),
                env("APPLICATION_NAME", "kcl-otel-sample"),
                region,
                env("OTLP_ENDPOINT", "https://otlp." + region + ".amazonaws.com/v1/metrics"),
                Long.parseLong(env("EXPORT_INTERVAL_MILLIS", "60000")),
                Integer.parseInt(env("PRODUCER_RECORDS_PER_SECOND", "10")),
                Integer.parseInt(env("PRODUCER_DURATION_SECONDS", "300")));
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    public String getStreamName() { return streamName; }
    public String getApplicationName() { return applicationName; }
    public String getRegion() { return region; }
    public String getOtlpEndpoint() { return otlpEndpoint; }
    public long getExportIntervalMillis() { return exportIntervalMillis; }
    public int getProducerRecordsPerSecond() { return producerRecordsPerSecond; }
    public int getProducerDurationSeconds() { return producerDurationSeconds; }
}
