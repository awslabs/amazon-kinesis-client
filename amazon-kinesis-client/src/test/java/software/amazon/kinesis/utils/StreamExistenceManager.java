package software.amazon.kinesis.utils;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.config.KCLAppConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Value
@Slf4j
public class StreamExistenceManager extends AWSResourceManager {
    private final KinesisAsyncClient client;
    private final KCLAppConfig testConfig;

    public StreamExistenceManager(KCLAppConfig config) throws URISyntaxException, IOException {
        this.testConfig = config;
        this.client = config.buildAsyncKinesisClient();
    }

    public boolean isResourceActive(String streamName) {
        final DescribeStreamSummaryRequest request = DescribeStreamSummaryRequest.builder().streamName(streamName).build();
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

    public void deleteResourceCall(String streamName) throws Exception {
        final DeleteStreamRequest request = DeleteStreamRequest.builder().streamName(streamName).enforceConsumerDeletion(true).build();
        client.deleteStream(request).get(30, TimeUnit.SECONDS);
    }

    public List<String> getAllResourceNames() throws Exception {
        ListStreamsRequest listStreamRequest = ListStreamsRequest.builder().build();
        List<String> allStreamNames = new ArrayList<>();
        ListStreamsResponse result = null;
        do {
            result = FutureUtils.resolveOrCancelFuture(client.listStreams(listStreamRequest), Duration.ofSeconds(60));
            allStreamNames.addAll(result.streamNames());
            listStreamRequest = ListStreamsRequest.builder().exclusiveStartStreamName(result.nextToken()).build();
        } while (result.hasMoreStreams());
        return allStreamNames;
    }

    public void checkStreamAndCreateIfNecessary(String streamName) {

        if (!isResourceActive(streamName)) {
            createStream(streamName, testConfig.getShardCount());
        }
        log.info("Using stream {} with region {}", streamName, testConfig.getRegion());
    }

    private void createStream(String streamName, int shardCount) {
        final CreateStreamRequest request = CreateStreamRequest.builder().streamName(streamName).shardCount(shardCount).build();
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
                boolean isActive = isResourceActive(streamName);
                if (isActive) {
                    log.info("Succesfully created the stream {}", streamName);
                    return;
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                } catch (InterruptedException e1) {
                    log.error("Failed to sleep");
                }
                log.info("Stream {} is not active yet, exception: ", streamName, e);
            }
        }
    }

}
