package software.amazon.kinesis.integration_tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
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
import software.amazon.kinesis.utils.RecordValidatorQueue;

import java.util.UUID;
import java.util.concurrent.*;

public class TestConsumerV2 extends TestConsumer {
    private static final Logger log = LoggerFactory.getLogger( TestConsumerV2.class );
    private final int outOfOrderError = -1;
    private final int missingRecordError = -2;
    private MetricsConfig metricsConfig;
    private RetrievalConfig retrievalConfig;
    private CheckpointConfig checkpointConfig;
    private CoordinatorConfig coordinatorConfig;
    private LeaseManagementConfig leaseManagementConfig;
    private LifecycleConfig lifecycleConfig;
    private ProcessorConfig processorConfig;

    public TestConsumerV2( KCLAppConfig consumerConfig ) {
        super( consumerConfig );
    }

    public void run() throws Exception {

        /**
         * Check if stream is created. If not, create it
         */
        StreamExistenceManager.newManager( this.consumerConfig ).checkStreamAndCreateIfNecessary( this.streamName );

        /**
         * Send dummy data to stream
         */
        ScheduledExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> producerFuture = producerExecutor.scheduleAtFixedRate( this::publishRecord, 10, 1, TimeUnit.SECONDS );

        RecordValidatorQueue recordValidator = new RecordValidatorQueue();

        /**
         * Setup configuration of KCL (including DynamoDB and CloudWatch)
         */
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region( region ).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region( region ).build();
        ConfigsBuilder configsBuilder = new ConfigsBuilder( streamName, streamName, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), new TestRecordProcessorFactoryV2( recordValidator ) );


        retrievalConfig = consumerConfig.getRetrievalConfig();
        checkpointConfig = configsBuilder.checkpointConfig();
        coordinatorConfig = configsBuilder.coordinatorConfig();
        leaseManagementConfig = configsBuilder.leaseManagementConfig()
                .initialPositionInStream( InitialPositionInStreamExtended.newInitialPosition( consumerConfig.getKclInitialPosition() ) )
                .initialLeaseTableReadCapacity( 50 ).initialLeaseTableWriteCapacity( 50 );
        lifecycleConfig = configsBuilder.lifecycleConfig();
        processorConfig = configsBuilder.processorConfig();
        metricsConfig = configsBuilder.metricsConfig();

        /**
         * Create Scheduler
         */
        Scheduler scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig
        );

        /**
         * Start record processing of dummy data
         */
        Thread schedulerThread = new Thread( scheduler );
        schedulerThread.setDaemon( true );
        schedulerThread.start();


        /**
         * Sleep for two minutes to allow the producer/consumer to run and then end the test case.
         */
        try {
            Thread.sleep( TimeUnit.SECONDS.toMillis( 60 * 2 ) ); // 60 * 2
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }

        /**
         * Stops sending dummy data.
         */
        log.info( "Cancelling producer and shutting down executor." );
        producerFuture.cancel( true );
        producerExecutor.shutdownNow();

        /**
         * Wait a few seconds for the last few records to be processed
         */
        Thread.sleep( TimeUnit.SECONDS.toMillis( 10 ) );

        /**
         * Stops consuming data. Finishes processing the current batch of data already received from Kinesis
         * before shutting down.
         */
        Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
        log.info( "Waiting up to 20 seconds for shutdown to complete." );
        try {
            gracefulShutdownFuture.get( 20, TimeUnit.SECONDS );
        } catch ( InterruptedException e ) {
            log.info( "Interrupted while waiting for graceful shutdown. Continuing." );
        } catch ( ExecutionException e ) {
            log.error( "Exception while executing graceful shutdown.", e );
        } catch ( TimeoutException e ) {
            log.error( "Timeout while waiting for shutdown.  Scheduler may not have exited." );
        }
        log.info( "Completed, shutting down now." );

        /**
         * Validate processed data
         */
        int errorVal = recordValidator.validateRecords( successfulPutRecords );
        if ( errorVal == outOfOrderError ) {
            throw new RuntimeException( "There was an error validating the records that were processed. The records were out of order" );
        } else if ( errorVal == missingRecordError ) {
            throw new RuntimeException( "There was an error validating the records that were processed. Some records were missing." );
        }
        log.info( "--------------Completed validation of processed records.--------------" );
    }
}
