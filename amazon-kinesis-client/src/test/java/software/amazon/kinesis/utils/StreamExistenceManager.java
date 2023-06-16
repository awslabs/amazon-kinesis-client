package software.amazon.kinesis.utils;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.config.KCLAppConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;

@Value
@Slf4j
public class StreamExistenceManager {
    private final KinesisAsyncClient client;
    private final KCLAppConfig testConfig;

    public StreamExistenceManager(KCLAppConfig config) throws URISyntaxException, IOException {
        this.testConfig = config;
        this.client = config.buildAsyncKinesisClient(config.getConsumerProtocol());
    }

    private boolean isStreamActive(String streamName) {

        DescribeStreamSummaryRequest request = DescribeStreamSummaryRequest.builder().streamName(streamName).build();

        final CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummaryResponseCompletableFuture = client.describeStreamSummary(request);

        try {
            final DescribeStreamSummaryResponse response = describeStreamSummaryResponseCompletableFuture.get(30, TimeUnit.SECONDS);
            boolean isActive = response.streamDescriptionSummary().streamStatus().equals(StreamStatus.ACTIVE);
            if (!isActive) {
                throw new RuntimeException("Stream is not active, instead in status: " + response.streamDescriptionSummary().streamStatus());
            }
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ResourceNotFoundException) {
                return false;
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createStream(String streamName, int shardCount) {
        CreateStreamRequest request = CreateStreamRequest.builder().streamName(streamName).shardCount(shardCount).build();
        try {
            client.createStream(request).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create stream with name " + streamName, e);
        }

        int i = 0;
        while (true) {
            i++;
            if (i > 100) {
                throw new RuntimeException("Failed stream creation, did not transition into active");
            }
            try {
                boolean isActive = isStreamActive(streamName);
                if (isActive) {
                    log.info("Succesfully created the stream " + streamName);
                    return;
                }
            } catch (Exception e) {
                try {
                    sleep(10_000); // 10 secs backoff.
                } catch (InterruptedException e1) {
                    log.error("Failed to sleep");
                }
                log.info("Stream {} is not active yet, exception: ", streamName, e);
            }
        }
    }

    public void deleteStream(String streamName) {
        DeleteStreamRequest request = DeleteStreamRequest.builder().streamName(streamName).enforceConsumerDeletion(true).build();
        try{
            client.deleteStream(request).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete stream with name " + streamName, e);
        }

        int i = 0;
        while (true) {
            i++;
            if (i > 100) {
                throw new RuntimeException("Failed stream deletion");
            }
            try {
                boolean isActive = isStreamActive(streamName);
                if (!isActive) {
                    log.info("Succesfully deleted the stream " + streamName);
                    return;
                }
            } catch (Exception e) {
                try {
                    sleep(10_000); // 10 secs backoff.
                } catch (InterruptedException e1) {}
                log.info("Stream {} is not deleted yet, exception: ", streamName, e);
            }
        }
    }

    public void checkStreamAndCreateIfNecessary(String streamName) {

        if (!isStreamActive(streamName)) {
            createStream(streamName, testConfig.getShardCount());
        }
        log.info("Using stream {} in endpoint {} with region {}", streamName, testConfig.getEndpoint(), testConfig.getRegion());
    }

}
