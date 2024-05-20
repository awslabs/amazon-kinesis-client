package software.amazon.kinesis.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.PutResourcePolicyRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.RetrievalMode;

@Value
@Slf4j
public class StreamExistenceManager extends AWSResourceManager {
    private static final int CHECK_RESOURCE_ACTIVE_MAX_RETRIES = 3;

    private final KinesisAsyncClient client;
    private final KCLAppConfig testConfig;

    public StreamExistenceManager(KCLAppConfig config) throws URISyntaxException, IOException {
        this.testConfig = config;
        this.client = config.buildAsyncKinesisClientForStreamOwner();
    }

    public boolean isResourceActive(String streamName) {
        final DescribeStreamSummaryRequest request =
                DescribeStreamSummaryRequest.builder().streamName(streamName).build();
        try {
            final DescribeStreamSummaryResponse response =
                    FutureUtils.resolveOrCancelFuture(client.describeStreamSummary(request), Duration.ofSeconds(60));
            final boolean isActive =
                    response.streamDescriptionSummary().streamStatus().equals(StreamStatus.ACTIVE);
            return isActive;
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

    private boolean isConsumerActive(Arn consumerArn) {
        final DescribeStreamConsumerRequest request = DescribeStreamConsumerRequest.builder()
                .consumerARN(consumerArn.toString())
                .build();
        try {
            final DescribeStreamConsumerResponse response =
                    FutureUtils.resolveOrCancelFuture(client.describeStreamConsumer(request), Duration.ofSeconds(60));
            final boolean isActive =
                    response.consumerDescription().consumerStatus().equals(ConsumerStatus.ACTIVE);
            return isActive;
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
        final DeleteStreamRequest request = DeleteStreamRequest.builder()
                .streamName(streamName)
                .enforceConsumerDeletion(true)
                .build();
        client.deleteStream(request).get(30, TimeUnit.SECONDS);
    }

    public List<String> getAllResourceNames() throws Exception {
        ListStreamsRequest listStreamRequest = ListStreamsRequest.builder().build();
        List<String> allStreamNames = new ArrayList<>();
        ListStreamsResponse result = null;
        do {
            result = FutureUtils.resolveOrCancelFuture(client.listStreams(listStreamRequest), Duration.ofSeconds(60));
            allStreamNames.addAll(result.streamNames());
            listStreamRequest = ListStreamsRequest.builder()
                    .exclusiveStartStreamName(result.nextToken())
                    .build();
        } while (result.hasMoreStreams());
        return allStreamNames;
    }

    public void checkStreamsAndCreateIfNecessary() {
        for (String streamName : testConfig.getStreamNames()) {
            if (!isResourceActive(streamName)) {
                createStream(streamName, testConfig.getShardCount());
            }
            log.info("Using stream {} with region {}", streamName, testConfig.getRegion());
        }

        if (testConfig.isCrossAccount()) {
            for (Arn streamArn : testConfig.getStreamArns()) {
                log.info("Putting cross account stream resource policy for stream {}", streamArn);
                putResourcePolicyForCrossAccount(
                        streamArn,
                        getCrossAccountStreamResourcePolicy(testConfig.getAccountIdForConsumer(), streamArn));
            }
        }
    }

    public Map<Arn, Arn> createCrossAccountConsumerIfNecessary() throws Exception {
        // For cross account, KCL cannot create the consumer automatically in another account, so
        // we have to create it ourselves and provide the arn to the StreamConfig in multi-stream mode or
        // RetrievalConfig in single-stream mode
        if (testConfig.isCrossAccount() && testConfig.getRetrievalMode().equals(RetrievalMode.STREAMING)) {
            final Map<Arn, Arn> streamToConsumerArnsMap = new HashMap<>();
            for (Arn streamArn : testConfig.getStreamArns()) {
                final Arn consumerArn =
                        registerConsumerAndWaitForActive(streamArn, KCLAppConfig.CROSS_ACCOUNT_CONSUMER_NAME);
                putResourcePolicyForCrossAccount(
                        consumerArn,
                        getCrossAccountConsumerResourcePolicy(testConfig.getAccountIdForConsumer(), consumerArn));
                streamToConsumerArnsMap.put(streamArn, consumerArn);
            }
            return streamToConsumerArnsMap;
        }
        return null;
    }

    private void putResourcePolicyForCrossAccount(Arn resourceArn, String policy) {
        try {
            final PutResourcePolicyRequest putResourcePolicyRequest = PutResourcePolicyRequest.builder()
                    .resourceARN(resourceArn.toString())
                    .policy(policy)
                    .build();
            FutureUtils.resolveOrCancelFuture(
                    client.putResourcePolicy(putResourcePolicyRequest), Duration.ofSeconds(60));
        } catch (Exception e) {
            throw new RuntimeException("Failed to PutResourcePolicy " + policy + " on resource " + resourceArn, e);
        }
    }

    private String getCrossAccountStreamResourcePolicy(String accountId, Arn streamArn) {
        return "{\"Version\":\"2012-10-17\","
                + "\"Statement\":[{"
                + "\"Effect\": \"Allow\","
                + "\"Principal\": {\"AWS\": \"" + accountId + "\"},"
                + "\"Action\": ["
                + "\"kinesis:DescribeStreamSummary\",\"kinesis:ListShards\",\"kinesis:PutRecord\",\"kinesis:PutRecords\","
                + "\"kinesis:GetRecords\",\"kinesis:GetShardIterator\"],"
                + "\"Resource\": \"" + streamArn.toString() + "\""
                + "}]}";
    }

    private String getCrossAccountConsumerResourcePolicy(String accountId, Arn consumerArn) {
        return "{\"Version\":\"2012-10-17\","
                + "\"Statement\":[{"
                + "\"Effect\": \"Allow\","
                + "\"Principal\": {\"AWS\": \"" + accountId + "\"},"
                + "\"Action\": ["
                + "\"kinesis:DescribeStreamConsumer\",\"kinesis:SubscribeToShard\"],"
                + "\"Resource\": \"" + consumerArn.toString() + "\""
                + "}]}";
    }

    private Arn registerConsumerAndWaitForActive(Arn streamArn, String consumerName) throws Exception {
        final RegisterStreamConsumerRequest registerStreamConsumerRequest = RegisterStreamConsumerRequest.builder()
                .streamARN(streamArn.toString())
                .consumerName(consumerName)
                .build();
        final RegisterStreamConsumerResponse response = FutureUtils.resolveOrCancelFuture(
                client.registerStreamConsumer(registerStreamConsumerRequest), Duration.ofSeconds(60));
        final Arn consumerArn = Arn.fromString(response.consumer().consumerARN());

        int retries = 0;
        while (!isConsumerActive(consumerArn)) {
            log.info("Consumer {} is not active yet. Checking again in 5 seconds.", consumerArn);
            if (retries > CHECK_RESOURCE_ACTIVE_MAX_RETRIES) {
                throw new RuntimeException("Failed consumer registration, did not transition into active");
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                log.error("Failed to sleep");
            }
            retries++;
        }
        log.info("Successfully registered consumer {}", consumerArn);
        return consumerArn;
    }

    private void createStream(String streamName, int shardCount) {
        final CreateStreamRequest request = CreateStreamRequest.builder()
                .streamName(streamName)
                .shardCount(shardCount)
                .build();
        try {
            client.createStream(request).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create stream with name " + streamName, e);
        }

        int retries = 0;
        while (!isResourceActive(streamName)) {
            log.info("Stream {} is not active yet. Checking again in 5 seconds.", streamName);
            if (retries > CHECK_RESOURCE_ACTIVE_MAX_RETRIES) {
                throw new RuntimeException("Failed stream creation, did not transition into active");
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                log.error("Failed to sleep");
            }
            retries++;
        }
        log.info("Successfully created the stream {}", streamName);
    }
}
