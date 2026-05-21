package software.amazon.kinesis.sample.otel;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsBackend;
import software.amazon.kinesis.metrics.MetricsLevel;

/**
 * KCL sample application demonstrating end-to-end OTel metrics flowing to CloudWatch.
 */
public class SampleApp {
    private static final Logger log = LoggerFactory.getLogger(SampleApp.class);

    private Scheduler scheduler;

    public static void main(String[] args) {
        SampleAppConfig config = SampleAppConfig.fromEnvironment();
        log.info("Starting KCL OTel Sample App: stream={}, region={}, otlpEndpoint={}",
                config.getStreamName(), config.getRegion(), config.getOtlpEndpoint());

        SampleApp app = new SampleApp();
        app.run(config);
    }

    public void run(SampleAppConfig config) {
        // 1. Initialize OTel SDK with OTLP exporter → CloudWatch
        var otelSdk = OTelSdkSetup.initialize(config);

        // 2. Build AWS clients
        Region region = Region.of(config.getRegion());
        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder().region(region));
        DynamoDbAsyncClient dynamoDbClient = DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();

        // 3. Build KCL configuration
        String workerId = UUID.randomUUID().toString();
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                config.getStreamName(),
                config.getApplicationName(),
                kinesisClient,
                dynamoDbClient,
                cloudWatchClient,
                workerId,
                new SampleRecordProcessorFactory());

        // 4. Build Scheduler with OTel metrics backend
        scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig()
                        .metricsBackend(MetricsBackend.OTEL)
                        .metricsLevel(MetricsLevel.DETAILED),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig());

        // 5. Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, initiating graceful shutdown...");
            Future<Boolean> gracefulShutdown = scheduler.startGracefulShutdown();
            try {
                gracefulShutdown.get(30, TimeUnit.SECONDS);
                log.info("KCL Scheduler shutdown complete");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.warn("Graceful shutdown did not complete in time", e);
            }
            OTelSdkSetup.shutdown(otelSdk);
            log.info("OTel SDK shutdown complete, all metrics flushed");
        }));

        // 6. Run scheduler (blocks until shutdown)
        log.info("Starting KCL Scheduler with worker ID: {}", workerId);
        scheduler.run();
    }
}
