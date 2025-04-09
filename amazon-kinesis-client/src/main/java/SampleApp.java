import java.util.UUID;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.worker.RecordProcessorFactory;

public class SampleApp {

    private final String streamName;
    private final Region region;
    private final KinesisAsyncClient kinesisClient;

    public static void main(String[] args) {
        String streamName = "DDB-Scan-usage-test";
        Region region = Region.US_EAST_1;
        new SampleApp(streamName, region).run();
    }

    public SampleApp(String streamName, Region region) {
        this.streamName = streamName;
        this.region = region;
        this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder().region(this.region));
    }

    public void run() {
        DynamoDbAsyncClient dynamoDbAsyncClient =
                DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient =
                CloudWatchAsyncClient.builder().region(region).build();

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                streamName,
                streamName,
                kinesisClient,
                dynamoDbAsyncClient,
                cloudWatchClient,
                UUID.randomUUID().toString(),
                new RecordProcessorFactory());
        LeaseManagementConfig leaseManagementConfig = configsBuilder.leaseManagementConfig();
        leaseManagementConfig.workerUtilizationAwareAssignmentConfig().varianceBalancingFrequency(5);

        // failoverTimeMillis = 10 min
        leaseManagementConfig.failoverTimeMillis(600000); // 10 minute
        //        RetrievalConfig config = configsBuilder.retrievalConfig();
        //        PollingConfig pollingConfig = new PollingConfig(config.kinesisClient());
        //
        //        //idleTimeBetweenReadsInMillis = 200
        //        pollingConfig.idleTimeBetweenReadsInMillis(200);
        //
        //        config.retrievalSpecificConfig();
        //
        //        //reBalanceThresholdPercentage = 4
        //        leaseManagementConfig.workerUtilizationAwareAssignmentConfig().reBalanceThresholdPercentage(4);
        //
        //        // maxGetRecordsThreadPool = 10 ??

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                leaseManagementConfig,
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig());

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }
}
