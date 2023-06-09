package software.amazon.kinesis.integration_tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.config.KCLAppConfig;
import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

public class TestConsumer {

    private static final Logger log = LoggerFactory.getLogger( TestConsumer.class );
    public final KCLAppConfig consumerConfig;
    public final Region region;
    public final String streamName;
    public final KinesisAsyncClient kinesisClient;
    public int successfulPutRecords = 0;
    public BigInteger payloadCounter = new BigInteger( "0" );


    public TestConsumer( KCLAppConfig consumerConfig ) {
        this.consumerConfig = consumerConfig;
        this.region = consumerConfig.getRegion();
        this.streamName = consumerConfig.getStreamName();
        this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient( KinesisAsyncClient.builder().region( this.region ) );
    }

    public void publishRecord() {
        PutRecordRequest request;
        try {
            request = PutRecordRequest.builder()
                    .partitionKey( RandomStringUtils.randomAlphabetic( 5, 20 ) )
                    .streamName( streamName )
                    .data( SdkBytes.fromByteBuffer( wrapWithCounter( 5, payloadCounter ) ) ) // 1024 is 1 KB
                    .build();
            kinesisClient.putRecord( request ).get();
            // Increment the payload counter if the putRecord call was successful
            payloadCounter = payloadCounter.add( new BigInteger( "1" ) );
            successfulPutRecords += 1;
        } catch ( InterruptedException e ) {
            log.info( "Interrupted, assuming shutdown." );
        } catch ( ExecutionException e ) {
            log.error( "Error during publishRecord. Will try again next cycle", e );
        } catch ( RuntimeException e ) {
            log.error( "Error while creating request", e );
        }
    }

    private ByteBuffer wrapWithCounter( int payloadSize, BigInteger payloadCounter ) throws RuntimeException {
        byte[] returnData;
        log.info( "--------------Putting record with data: {}", payloadCounter );
        ObjectMapper mapper = new ObjectMapper();
        try {
            returnData = mapper.writeValueAsBytes( payloadCounter );
        } catch ( Exception e ) {
            log.error( "Error creating payload data for {}", payloadCounter.toString() );
            throw new RuntimeException( "Error converting object to bytes: ", e );
        }
        return ByteBuffer.wrap( returnData );
    }

}
