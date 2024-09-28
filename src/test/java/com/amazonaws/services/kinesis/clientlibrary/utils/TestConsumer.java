package com.amazonaws.services.kinesis.clientlibrary.utils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.regions.Region;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.clientlibrary.config.KCLAppConfig;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class TestConsumer {
    private KinesisClientLibConfiguration kclConfig;
    private Worker worker;
    public final KCLAppConfig consumerConfig;
    public final Region region;
    public final String streamName;
    public final AmazonKinesis kinesisClient;
    public final AmazonDynamoDB dynamoClient;
    private ScheduledExecutorService producerExecutor;
    private ScheduledFuture<?> producerFuture;
    private ScheduledExecutorService consumerExecutor;
    private ScheduledFuture<?> consumerFuture;
    private final ObjectMapper mapper = new ObjectMapper();
    public int successfulPutRecords = 0;
    public BigInteger payloadCounter = new BigInteger("0");

    public TestConsumer(KCLAppConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
        this.region = consumerConfig.getRegion();
        this.streamName = consumerConfig.getStreamName();

        try {
            this.kinesisClient = consumerConfig.buildSyncKinesisClient();
            this.dynamoClient = consumerConfig.buildSyncDynamoDbClient();
            this.kclConfig = consumerConfig.getKclConfiguration();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    public void run() throws Exception {

        StreamExistenceManager streamExistenceManager = new StreamExistenceManager(this.consumerConfig);
        LeaseTableManager leaseTableManager = new LeaseTableManager(this.dynamoClient);

        // Clean up any old streams or lease tables left in test environment
        cleanTestResources(streamExistenceManager, leaseTableManager);

        // Create new stream for test case
        streamExistenceManager.createStream(this.streamName, this.consumerConfig.getShardCount());

        // Send dummy data to stream
        startProducer();
        setupConsumerResources();

        try {
            // Start record processing of dummy data
            startConsumer();

            // Sleep for three minutes to allow the producer/consumer to run and then end the test case.
            Thread.sleep(TimeUnit.SECONDS.toMillis(60 * 3 ));

            // Stops sending dummy data.
            stopProducer();

            // Wait a few seconds for the last few records to be processed
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));

            // Finishes processing current batch of data already received from Kinesis before shutting down.
            awaitConsumerFinish();

            // Validate processed data
            validateRecordProcessor();

        } catch (Exception e) {
            // Test Failed. Throw exception and clean resources
            log.info("----------Test Failed: Cleaning up resources-----------");
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
        this.producerFuture = producerExecutor.scheduleAtFixedRate(this::publishRecord, 60, 1, TimeUnit.SECONDS);
    }

    private void setupConsumerResources() throws Exception {
        final TestRecordProcessorFactory recordProcessorFactory =
                new TestRecordProcessorFactory( consumerConfig.getRecordValidator() );

        worker = new Worker.Builder()
                .kinesisClient( consumerConfig.buildSyncKinesisClient() )
                .dynamoDBClient( dynamoClient )
                .cloudWatchClient( consumerConfig.buildSyncCloudWatchClient() )
                .recordProcessorFactory( recordProcessorFactory ).config( kclConfig )
                .build();
    }

    private void startConsumer() {
        // Start record processing of dummy data
        this.consumerExecutor = Executors.newSingleThreadScheduledExecutor();
        this.consumerFuture = consumerExecutor.schedule(worker, 0, TimeUnit.SECONDS);
    }

    private void stopProducer() {
        log.info("Cancelling producer and shutting down executor.");
        producerFuture.cancel(false);
        producerExecutor.shutdown();
    }

    private void awaitConsumerFinish() throws Exception{
        Future<Boolean> gracefulShutdownFuture = worker.startGracefulShutdown();
        log.info("Waiting up to 20 seconds for shutdown to complete.");
        try {
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted while waiting for graceful shutdown. Continuing.");
        } catch (ExecutionException | TimeoutException e) {
            throw e;
        }
        log.info("Completed, shutting down now.");
    }

    private void validateRecordProcessor() {
        log.info("The number of expected records is: {}", successfulPutRecords);
        RecordValidationStatus errorVal = consumerConfig.getRecordValidator().validateRecords(successfulPutRecords);
        if (errorVal == RecordValidationStatus.OUT_OF_ORDER) {
            throw new RuntimeException("There was an error validating the records that were processed. The records were out of order");
        } else if (errorVal == RecordValidationStatus.MISSING_RECORD) {
            throw new RuntimeException("There was an error validating the records that were processed. Some records were missing.");
        }
        log.info("--------------Completed validation of processed records.--------------");
    }

    public void publishRecord() {
        PutRecordRequest request;
        request = new PutRecordRequest();
        request.setPartitionKey(RandomStringUtils.randomAlphabetic(5, 20));
        request.setStreamName(streamName);
        request.setData(wrapWithCounter(5, payloadCounter));

        kinesisClient.putRecord(request);

        // Increment the payload counter if the putRecord call was successful
        payloadCounter = payloadCounter.add(new BigInteger("1"));
        successfulPutRecords += 1;
    }

    private ByteBuffer wrapWithCounter(int payloadSize, BigInteger payloadCounter) throws RuntimeException {
        byte[] returnData;
        log.info("--------------Putting record with data: {}", payloadCounter);
        try {
            returnData = mapper.writeValueAsBytes(payloadCounter);
        } catch (Exception e) {
            throw new RuntimeException("Error converting object to bytes: ", e);
        }
        return ByteBuffer.wrap(returnData);
    }

    private void deleteResources(StreamExistenceManager streamExistenceManager, LeaseTableManager leaseTableManager) throws Exception {
        log.info("-------------Start deleting stream.----------------");
        streamExistenceManager.deleteResource(this.streamName);
        log.info("-------------Start deleting lease table.----------------");
        leaseTableManager.deleteResource(consumerConfig.getStreamName());
        log.info("-------------Finished deleting test resources.----------------");
    }
}