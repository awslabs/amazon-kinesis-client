package software.amazon.kinesis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

public class ApplicationTest {
    private static final Logger log = LoggerFactory.getLogger(ApplicationTest.class);

    /**
     * Invoke the main method
     * Verifies valid inputs and then starts running the app.
     */
    public static void main(String... args) {
        String streamName1 = "consumer-level-metrics-1";
        String streamName2 = "consumer-level-metrics-2";
        String region = "us-east-2";
        String applicationName = "consumer-level-metrics-test-new";

        new ApplicationTest(applicationName, streamName1, streamName2, region).run();
    }

    private final String applicationName;
    private final String streamName1;
    private final String streamName2;
    private final Region region;
    private final KinesisAsyncClient kinesisClient;

    /**
     * Constructor sets streamName and region. It also creates a KinesisClient object to send data to Kinesis.
     * This KinesisClient is used to send dummy data so that the consumer has something to read; it is also used
     * indirectly by the KCL to handle the consumption of the data.
     */
    private ApplicationTest(String applicationName, String streamName1, String streamName2, String region) {
        this.applicationName = applicationName;
        this.streamName1 = streamName1;
        this.streamName2 = streamName2;
        this.region = Region.of(ObjectUtils.firstNonNull(region, "us-east-2"));
        this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(this.region));
    }

    private void run() {

        /**
         * Sends dummy data to Kinesis. Not relevant to consuming the data with the KCL
         */
        ScheduledExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> producerFuture = producerExecutor.scheduleAtFixedRate(this::publishRecord, 1, 1, TimeUnit.SECONDS);

        /**
         * Sets up configuration for the KCL, including DynamoDB and CloudWatch dependencies. The final argument, a
         * ShardRecordProcessorFactory, is where the logic for record processing lives, and is located in a private
         * class below.
         */
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
        ConfigsBuilder configsBuilder1 = new ConfigsBuilder(streamName1, applicationName, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), new SampleRecordProcessorFactory());
        ConfigsBuilder configsBuilder2 = new ConfigsBuilder(streamName2, applicationName, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), new SampleRecordProcessorFactory());

        /**
         * The Scheduler (also called Worker in earlier versions of the KCL) is the entry point to the KCL. This
         * instance is configured with defaults provided by the ConfigsBuilder.
         */
        Scheduler scheduler1 = new Scheduler(
                configsBuilder1.checkpointConfig(),
                configsBuilder1.coordinatorConfig(),
                configsBuilder1.leaseManagementConfig(),
                configsBuilder1.lifecycleConfig(),
                configsBuilder1.metricsConfig(),
                configsBuilder1.processorConfig(),
                configsBuilder1.retrievalConfig().retrievalSpecificConfig(new PollingConfig(streamName1, kinesisClient))
        );
        Scheduler scheduler2 = new Scheduler(
                configsBuilder2.checkpointConfig(),
                configsBuilder2.coordinatorConfig(),
                configsBuilder2.leaseManagementConfig(),
                configsBuilder2.lifecycleConfig(),
                configsBuilder2.metricsConfig(),
                configsBuilder2.processorConfig(),
                configsBuilder2.retrievalConfig().retrievalSpecificConfig(new PollingConfig(streamName2, kinesisClient))
        );

        /**
         * Kickoff the Scheduler. Record processing of the stream of dummy data will continue indefinitely
         * until an exit is triggered.
         */
        Thread schedulerThread1 = new Thread(scheduler1);
        schedulerThread1.setDaemon(true);
        schedulerThread1.start();

        Thread schedulerThread2 = new Thread(scheduler2);
        schedulerThread2.setDaemon(true);
        schedulerThread2.start();

        /**
         * Allows termination of app by pressing Enter.
         */
        System.out.println("Press enter to shutdown");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            reader.readLine();
        } catch (IOException ioex) {
            log.error("Caught exception while waiting for confirm. Shutting down.", ioex);
        }

        /**
         * Stops sending dummy data.
         */
        log.info("Cancelling producer and shutting down executor.");
        producerFuture.cancel(true);
        producerExecutor.shutdownNow();

        /**
         * Stops consuming data. Finishes processing the current batch of data already received from Kinesis
         * before shutting down.
         */
        Future<Boolean> gracefulShutdownFuture1 = scheduler1.startGracefulShutdown();
        Future<Boolean> gracefulShutdownFuture2 = scheduler2.startGracefulShutdown();
        log.info("Waiting up to 20 seconds for shutdown to complete.");
        try {
            gracefulShutdownFuture1.get(20, TimeUnit.SECONDS);
            gracefulShutdownFuture2.get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted while waiting for graceful shutdown. Continuing.");
        } catch (ExecutionException e) {
            log.error("Exception while executing graceful shutdown.", e);
        } catch (TimeoutException e) {
            log.error("Timeout while waiting for shutdown.  Scheduler may not have exited.");
        }
        log.info("Completed, shutting down now.");
    }

    /**
     * Sends a single record of dummy data to Kinesis.
     */
    private void publishRecord() {
        PutRecordRequest request1 = PutRecordRequest.builder()
                .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                .streamName(streamName2)
                .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(10)))
                .build();
        PutRecordRequest request2 = PutRecordRequest.builder()
                .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                .streamName(streamName1)
                .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(1)))
                .build();
        PutRecordRequest request3 = PutRecordRequest.builder()
                .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                .streamName(streamName1)
                .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(1)))
                .build();
        PutRecordRequest request4 = PutRecordRequest.builder()
                .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                .streamName(streamName1)
                .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(1)))
                .build();
        PutRecordRequest request5 = PutRecordRequest.builder()
                .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                .streamName(streamName1)
                .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(1)))
                .build();
        PutRecordRequest request6 = PutRecordRequest.builder()
                .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                .streamName(streamName1)
                .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(1)))
                .build();
        try {
            kinesisClient.putRecord(request1).get();
            kinesisClient.putRecord(request2).get();
            kinesisClient.putRecord(request3).get();
            kinesisClient.putRecord(request4).get();
            kinesisClient.putRecord(request5).get();
            kinesisClient.putRecord(request6).get();
            System.out.println("600 records published yeah");
        } catch (InterruptedException e) {
            log.info("Interrupted, assuming shutdown.");
        } catch (ExecutionException e) {
            log.error("Exception while sending data to Kinesis. Will try again next cycle.", e);
        }
    }

    private static class SampleRecordProcessorFactory implements ShardRecordProcessorFactory {
        public ShardRecordProcessor shardRecordProcessor() {
            return new SampleRecordProcessor();
        }
    }

    /**
     * The implementation of the ShardRecordProcessor interface is where the heart of the record processing logic lives.
     * In this example all we do to 'process' is log info about the records.
     */
    private static class SampleRecordProcessor implements ShardRecordProcessor {

        private static final String SHARD_ID_MDC_KEY = "ShardId";

        private static final Logger log = LoggerFactory.getLogger(SampleRecordProcessor.class);

        private String shardId;

        /**
         * Invoked by the KCL before data records are delivered to the ShardRecordProcessor instance (via
         * processRecords). In this example we do nothing except some logging.
         *
         * @param initializationInput Provides information related to initialization.
         */
        public void initialize(InitializationInput initializationInput) {
            shardId = initializationInput.shardId();
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        /**
         * Handles record processing logic. The Amazon Kinesis Client Library will invoke this method to deliver
         * data records to the application. In this example we simply log our records.
         *
         * @param processRecordsInput Provides the records to be processed as well as information and capabilities
         *                            related to them (e.g. checkpointing).
         */
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Processing {} record(s)", processRecordsInput.records().size());
                processRecordsInput.records().forEach(r -> log.info("Processing record pk: {} -- Seq: {}", r.partitionKey(), r.sequenceNumber()));
            } catch (Throwable t) {
                log.error("Caught throwable while processing records. Aborting.");
                Runtime.getRuntime().halt(1);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        /** Called when the lease tied to this record processor has been lost. Once the lease has been lost,
         * the record processor can no longer checkpoint.
         *
         * @param leaseLostInput Provides access to functions and data related to the loss of the lease.
         */
        public void leaseLost(LeaseLostInput leaseLostInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Lost lease, so terminating.");
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        /**
         * Called when all data on this shard has been processed. Checkpointing must occur in the method for record
         * processing to be considered complete; an exception will be thrown otherwise.
         *
         * @param shardEndedInput Provides access to a checkpointer method for completing processing of the shard.
         */
        public void shardEnded(ShardEndedInput shardEndedInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Reached shard end checkpointing.");
                shardEndedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at shard end. Giving up.", e);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        /**
         * Invoked when Scheduler has been requested to shut down (i.e. we decide to stop running the app by pressing
         * Enter). Checkpoints and logs the data a final time.
         *
         * @param shutdownRequestedInput Provides access to a checkpointer, allowing a record processor to checkpoint
         *                               before the shutdown is completed.
         */
        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Scheduler is shutting down, checkpointing.");
                shutdownRequestedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }
    }
}
