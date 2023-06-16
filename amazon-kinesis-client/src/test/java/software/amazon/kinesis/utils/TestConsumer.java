package software.amazon.kinesis.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

@Slf4j
public class TestConsumer {
    public final KCLAppConfig consumerConfig;
    public final Region region;
    public final String streamName;
    public final KinesisAsyncClient kinesisClient;
    private MetricsConfig metricsConfig;
    private RetrievalConfig retrievalConfig;
    private CheckpointConfig checkpointConfig;
    private CoordinatorConfig coordinatorConfig;
    private LeaseManagementConfig leaseManagementConfig;
    private LifecycleConfig lifecycleConfig;
    private ProcessorConfig processorConfig;
    public int successfulPutRecords = 0;
    public BigInteger payloadCounter = new BigInteger("0");

    public TestConsumer(KCLAppConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
        this.region = consumerConfig.getRegion();
        this.streamName = consumerConfig.getStreamName();
        this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(this.region));
    }

    public void run() throws Exception {

        // Check if stream is created. If not, create it
        StreamExistenceManager streamExistenceManager = new StreamExistenceManager(this.consumerConfig);
        streamExistenceManager.checkStreamAndCreateIfNecessary(this.streamName);

        // Send dummy data to stream
        ScheduledExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> producerFuture = producerExecutor.scheduleAtFixedRate(this::publishRecord, 60, 1, TimeUnit.SECONDS);

        RecordValidatorQueue recordValidator = new RecordValidatorQueue();

        // Setup configuration of KCL (including DynamoDB and CloudWatch)
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
        ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName, streamName, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), new TestRecordProcessorFactory(recordValidator));


        retrievalConfig = consumerConfig.getRetrievalConfig();
        checkpointConfig = configsBuilder.checkpointConfig();
        coordinatorConfig = configsBuilder.coordinatorConfig();
        leaseManagementConfig = configsBuilder.leaseManagementConfig()
                .initialPositionInStream(InitialPositionInStreamExtended.newInitialPosition(consumerConfig.getInitialPosition()))
                .initialLeaseTableReadCapacity(50).initialLeaseTableWriteCapacity(50);
        lifecycleConfig = configsBuilder.lifecycleConfig();
        processorConfig = configsBuilder.processorConfig();
        metricsConfig = configsBuilder.metricsConfig();

        // Create Scheduler
        Scheduler scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig
        );

        try {
            // Start record processing of dummy data
            Thread schedulerThread = new Thread(scheduler);
            schedulerThread.setDaemon(true);
            schedulerThread.start();

            // Sleep for two minutes to allow the producer/consumer to run and then end the test case.
            Thread.sleep(TimeUnit.SECONDS.toMillis(60 * 3));

            // Stops sending dummy data.
            log.info("Cancelling producer and shutting down executor.");
            producerFuture.cancel(false);
            producerExecutor.shutdown();

            // Wait a few seconds for the last few records to be processed
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));

            // Finishes processing current batch of data already received from Kinesis before shutting down.
            Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
            log.info("Waiting up to 20 seconds for shutdown to complete.");
            try {
                gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.info("Interrupted while waiting for graceful shutdown. Continuing.");
            } catch (ExecutionException e) {
                throw new ExecutionException("Exception while executing graceful shutdown. {}", e);
            } catch (TimeoutException e) {
                throw new TimeoutException("Timeout while waiting for shutdown.  Scheduler may not have exited. {}" + e);
            }
            log.info("Completed, shutting down now.");

            // Validate processed data
            log.info("The number of expected records is: {}", successfulPutRecords);
            RecordValidationStatus errorVal = recordValidator.validateRecords(successfulPutRecords);
            if (errorVal == RecordValidationStatus.OUT_OF_ORDER) {
                throw new RuntimeException("There was an error validating the records that were processed. The records were out of order");
            } else if (errorVal == RecordValidationStatus.MISSING_RECORD) {
                throw new RuntimeException("There was an error validating the records that were processed. Some records were missing.");
            }
            log.info("--------------Completed validation of processed records.--------------");

            // Clean up resources created
            Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            deleteResources(streamExistenceManager, dynamoClient);

        } catch (Exception e) {
            // Test Failed. Clean up resources and then throw exception.
            Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            deleteResources(streamExistenceManager, dynamoClient);
            throw e;
        }
    }

    public void publishRecord() {
        PutRecordRequest request;
        try {
            request = PutRecordRequest.builder()
                    .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                    .streamName(streamName)
                    .data(SdkBytes.fromByteBuffer(wrapWithCounter(5, payloadCounter))) // 1024 is 1 KB
                    .build();
            kinesisClient.putRecord(request).get();

            // Increment the payload counter if the putRecord call was successful
            payloadCounter = payloadCounter.add(new BigInteger("1"));
            successfulPutRecords += 1;
            log.info("---------Record published, successfulPutRecords is now: {}", successfulPutRecords);
        } catch (InterruptedException e) {
            log.info("Interrupted, assuming shutdown.");
        } catch (ExecutionException e) {
            log.error("Error during publishRecord. Will try again next cycle", e);
        } catch (RuntimeException e) {
            log.error("Error while creating request", e);
        }
    }

    private ByteBuffer wrapWithCounter(int payloadSize, BigInteger payloadCounter) throws RuntimeException {
        byte[] returnData;
        log.info("--------------Putting record with data: {}", payloadCounter);
        ObjectMapper mapper = new ObjectMapper();
        try {
            returnData = mapper.writeValueAsBytes(payloadCounter);
        } catch (Exception e) {
            log.error("Error creating payload data for {}", payloadCounter.toString());
            throw new RuntimeException("Error converting object to bytes: ", e);
        }
        return ByteBuffer.wrap(returnData);
    }

    private void deleteResources(StreamExistenceManager streamExistenceManager, DynamoDbAsyncClient dynamoDBClient) throws Exception {
        log.info("-------------Start deleting test resources.----------------");
        streamExistenceManager.deleteStream(this.streamName);
        deleteLeaseTable(dynamoDBClient, consumerConfig.getStreamName());
    }

    private void deleteLeaseTable(DynamoDbAsyncClient dynamoClient, String tableName) throws Exception {
        DeleteTableRequest request = DeleteTableRequest.builder().tableName(tableName).build();
        try {
            FutureUtils.resolveOrCancelFuture(dynamoClient.deleteTable(request), Duration.ofSeconds(60));
        } catch (ExecutionException e) {
            throw new Exception("Could not delete lease table: {}", e);
        } catch (InterruptedException e) {
            throw new Exception("Deleting lease table interrupted: {}", e);
        }

    }
}
