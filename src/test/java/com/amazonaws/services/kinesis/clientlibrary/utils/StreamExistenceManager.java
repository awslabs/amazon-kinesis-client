package com.amazonaws.services.kinesis.clientlibrary.utils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.clientlibrary.config.KCLAppConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import static java.lang.Thread.sleep;

@Value
@Slf4j
public class StreamExistenceManager extends AWSResourceManager {
    private final AmazonKinesis client;
    private final KCLAppConfig testConfig;
    private static final String ACTIVE_STREAM_STATE = "ACTIVE";

    public StreamExistenceManager(KCLAppConfig config) throws URISyntaxException, IOException {
        this.testConfig = config;
        this.client = config.buildSyncKinesisClient();
    }

    public boolean isResourceActive(String streamName) {

        DescribeStreamSummaryRequest request = new DescribeStreamSummaryRequest();
        request.setStreamName(streamName);

        try {
            final DescribeStreamSummaryResult response = client.describeStreamSummary(request);
            String streamStatus = response.getStreamDescriptionSummary().getStreamStatus();
            boolean isActive = streamStatus.equals(ACTIVE_STREAM_STATE);
            if (!isActive) {
                throw new RuntimeException("Stream is not active, instead in status: " + response.getStreamDescriptionSummary().getStreamStatus());
            }
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteResourceCall(String streamName) {
        DeleteStreamRequest request = new DeleteStreamRequest();
        request.setStreamName(streamName);
        request.withEnforceConsumerDeletion(true);
        client.deleteStream(request);
    }

    public List<String> getAllResourceNames() throws Exception {
        ListStreamsRequest listStreamRequest = new ListStreamsRequest();
        List<String> allStreamNames = new ArrayList<>();
        ListStreamsResult result = null;
        do {
            result = client.listStreams(listStreamRequest);
            allStreamNames.addAll(result.getStreamNames());
            listStreamRequest = listStreamRequest.withExclusiveStartStreamName(result.getNextToken());
        } while (result.getHasMoreStreams());
        return allStreamNames;
    }

    public void createStream(String streamName, int shardCount) {
        CreateStreamRequest request = new CreateStreamRequest();
        request.setStreamName(streamName);
        request.setShardCount(shardCount);

        try {
            client.createStream(request);
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

}