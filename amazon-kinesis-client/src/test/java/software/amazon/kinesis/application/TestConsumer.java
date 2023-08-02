package software.amazon.kinesis.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountResponse;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.utils.LeaseTableManager;
import software.amazon.kinesis.utils.RecordValidationStatus;
import software.amazon.kinesis.utils.ReshardOptions;
import software.amazon.kinesis.utils.StreamExistenceManager;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private Scheduler scheduler;
    private ScheduledExecutorService producerExecutor;
    private ScheduledFuture<?> producerFuture;
    private ScheduledExecutorService consumerExecutor;
    private ScheduledFuture<?> consumerFuture;
    private DynamoDbAsyncClient dynamoClient;
    private final ObjectMapper mapper = new ObjectMapper();
    public int successfulPutRecords = 0;
    public BigInteger payloadCounter = new BigInteger("0");

    public TestConsumer(KCLAppConfig consumerConfig) throws Exception {
        this.consumerConfig = consumerConfig;
        this.region = consumerConfig.getRegion();
        this.streamName = consumerConfig.getStreamName();
        this.kinesisClient = consumerConfig.buildAsyncKinesisClient();
        this.dynamoClient = consumerConfig.buildAsyncDynamoDbClient();
    }

    public void run() throws Exception {

        final StreamExistenceManager streamExistenceManager = new StreamExistenceManager(this.consumerConfig);
        final LeaseTableManager leaseTableManager = new LeaseTableManager(this.dynamoClient);

        // Clean up any old streams or lease tables left in test environment
        cleanTestResources(streamExistenceManager, leaseTableManager);

        // Check if stream is created. If not, create it
        streamExistenceManager.checkStreamAndCreateIfNecessary(this.streamName);

        startProducer();
        setUpConsumerResources();

        try {
            startConsumer();

            // Sleep to allow the producer/consumer to run and then end the test case. If non-reshard sleep 3 minutes, else sleep 4 minutes per scale.
            final int sleepMinutes = (consumerConfig.getReshardFactorList() == null) ? 3 : (4 * consumerConfig.getReshardFactorList().size());
            Thread.sleep(TimeUnit.MINUTES.toMillis(sleepMinutes));

            // Stops sending dummy data.
            stopProducer();

            // Wait a few seconds for the last few records to be processed
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));

            // Finishes processing current batch of data already received from Kinesis before shutting down.
            awaitConsumerFinish();

            // Validate processed data
            validateRecordProcessor();

        } catch (Exception e) {
            // Test Failed. Clean up resources and then throw exception.
            log.info("----------Test Failed: Cleaning up resources------------");
            throw e;
        } finally {
            // Clean up resources created
            deleteResources(streamExistenceManager, leaseTableManager);
        }
    }

    private void cleanTestResources(StreamExistenceManager streamExistenceManager, LeaseTableManager leaseTableManager) throws Exception {
        log.info("----------Before starting, Cleaning test environment----------");
        log.info("----------Deleting all lease tables in account----------");
        leaseTableManager.deleteAllResource();
        log.info("----------Finished deleting all lease tables-------------");

        log.info("----------Deleting all streams in account----------");
        streamExistenceManager.deleteAllResource();
        log.info("----------Finished deleting all streams-------------");
    }

    private void startProducer() {
        this.producerExecutor = Executors.newSingleThreadScheduledExecutor();
        this.producerFuture = producerExecutor.scheduleAtFixedRate(this::publishRecord, 10, 1, TimeUnit.SECONDS);

        // Reshard logic if required for the test
        if (consumerConfig.getReshardFactorList() != null) {
            log.info("----Reshard Config found: {}", consumerConfig.getReshardFactorList());

            final StreamScaler s = new StreamScaler(kinesisClient, consumerConfig.getStreamName(), consumerConfig.getReshardFactorList(), consumerConfig);

            // Schedule the stream scales 4 minutes apart with 2 minute starting delay
            for (int i = 0; i < consumerConfig.getReshardFactorList().size(); i++) {
                producerExecutor.schedule(s, (4 * i) + 2, TimeUnit.MINUTES);
            }
        }
    }

    private void setUpConsumerResources() throws Exception {
        // Setup configuration of KCL (including DynamoDB and CloudWatch)
        final ConfigsBuilder configsBuilder = consumerConfig.getConfigsBuilder();

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
        this.scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig
        );
    }

    private void startConsumer() {
        // Start record processing of dummy data
        this.consumerExecutor = Executors.newSingleThreadScheduledExecutor();
        this.consumerFuture = consumerExecutor.schedule(scheduler, 0, TimeUnit.SECONDS);
    }

    public void stopProducer() {
        log.info("Cancelling producer and shutting down executor.");
        if (producerFuture != null) {
            producerFuture.cancel(false);
        }
        if (producerExecutor != null) {
            producerExecutor.shutdown();
        }
    }

    public void publishRecord() {
        final PutRecordRequest request;
        try {
            request = PutRecordRequest.builder()
                    .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                    .streamName(this.streamName)
                    .data(SdkBytes.fromByteBuffer(wrapWithCounter(5, payloadCounter))) // 1024 is 1 KB
                    .build();
            kinesisClient.putRecord(request).get();

            // Increment the payload counter if the putRecord call was successful
            payloadCounter = payloadCounter.add(new BigInteger("1"));
            successfulPutRecords += 1;
            log.info("---------Record published, successfulPutRecords is now: {}", successfulPutRecords);
        } catch (InterruptedException e) {
            log.info("Interrupted, assuming shutdown. ", e);
        } catch (ExecutionException | RuntimeException e) {
            log.error("Error during publish records", e);
        }
    }

    private ByteBuffer wrapWithCounter(int payloadSize, BigInteger payloadCounter) throws RuntimeException {
        final byte[] returnData;
        log.info("---------Putting record with data: {}", payloadCounter);
        try {
            returnData = mapper.writeValueAsBytes(payloadCounter);
        } catch (Exception e) {
            throw new RuntimeException("Error converting object to bytes: ", e);
        }
        return ByteBuffer.wrap(returnData);
    }

    private void awaitConsumerFinish() throws Exception {
        Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
        log.info("Waiting up to 20 seconds for shutdown to complete.");
        try {
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted while waiting for graceful shutdown. Continuing.");
        } catch (ExecutionException | TimeoutException e) {
            scheduler.shutdown();
        }
        log.info("Completed, shutting down now.");
    }

    private void validateRecordProcessor() throws Exception {
        log.info("The number of expected records is: {}", successfulPutRecords);
        final RecordValidationStatus errorVal = consumerConfig.getRecordValidator().validateRecords(successfulPutRecords);
        if (errorVal != RecordValidationStatus.NO_ERROR) {
            throw new RuntimeException("There was an error validating the records that were processed: " + errorVal.toString());
        }
        log.info("---------Completed validation of processed records.---------");
    }

    private void deleteResources(StreamExistenceManager streamExistenceManager, LeaseTableManager leaseTableManager) throws Exception {
        log.info("-------------Start deleting stream.---------");
        streamExistenceManager.deleteResource(this.streamName);
        log.info("---------Start deleting lease table.---------");
        leaseTableManager.deleteResource(this.consumerConfig.getStreamName());
        log.info("---------Finished deleting resources.---------");
    }

    @Data
    private static class StreamScaler implements Runnable {
        private final KinesisAsyncClient client;
        private final String streamName;
        private final List<ReshardOptions> scalingFactors;
        private final KCLAppConfig consumerConfig;
        private int scalingFactorIdx = 0;
        private DescribeStreamSummaryRequest describeStreamSummaryRequest;

        private synchronized void scaleStream() throws InterruptedException, ExecutionException {
            final DescribeStreamSummaryResponse response = client.describeStreamSummary(describeStreamSummaryRequest).get();

            final int openShardCount = response.streamDescriptionSummary().openShardCount();
            final int targetShardCount = scalingFactors.get(scalingFactorIdx).calculateShardCount(openShardCount);

            log.info("Scaling stream {} from {} shards to {} shards w/ scaling factor {}",
                    streamName, openShardCount, targetShardCount, scalingFactors.get(scalingFactorIdx));

            final UpdateShardCountRequest updateShardCountRequest = UpdateShardCountRequest.builder()
                    .streamName(streamName).targetShardCount(targetShardCount).scalingType(ScalingType.UNIFORM_SCALING).build();
            final UpdateShardCountResponse shardCountResponse = client.updateShardCount(updateShardCountRequest).get();
            log.info("Executed shard scaling request. Response Details : {}", shardCountResponse.toString());

            scalingFactorIdx++;
        }

        @Override
        public void run() {
            if (scalingFactors.size() == 0 || scalingFactorIdx >= scalingFactors.size()) {
                log.info("No scaling factor found in list");
                return;
            }
            log.info("Starting stream scaling with params : {}", this);

            if (describeStreamSummaryRequest == null) {
                describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder().streamName(streamName).build();
            }
            try {
                scaleStream();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Caught error while scaling shards for stream", e);
            } finally {
                log.info("Reshard List State : {}", scalingFactors);
            }
        }
    }
}
