package software.amazon.kinesis.retrieval;

import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.checkpoint.SentinelCheckpoint;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class IteratorBuilderTest {

    private static final String SHARD_ID = "Shard-001";
    private static final String STREAM_NAME = "Stream";
    private static final String CONSUMER_ARN = "arn:stream";
    private static final Instant TIMESTAMP = Instant.parse("2018-04-26T13:03:00Z");
    private static final String SEQUENCE_NUMBER = "1234";

    @Test
    public void subscribeLatestTest() {
        latestTest(this::stsBase, this::verifyStsBase, IteratorBuilder::request, WrappedRequest::wrapped);
    }

    @Test
    public void getShardLatestTest() {
        latestTest(this::gsiBase, this::verifyGsiBase, IteratorBuilder::request, WrappedRequest::wrapped);
    }

    @Test
    public void subscribeTrimTest() {
        trimHorizonTest(this::stsBase, this::verifyStsBase, IteratorBuilder::request, WrappedRequest::wrapped);
    }

    @Test
    public void getShardTrimTest() {
        trimHorizonTest(this::gsiBase, this::verifyGsiBase, IteratorBuilder::request, WrappedRequest::wrapped);
    }

    @Test
    public void subscribeSequenceNumberTest() {
        sequenceNumber(this::stsBase, this::verifyStsBase, IteratorBuilder::request, WrappedRequest::wrapped);
    }

    @Test
    public void subscribeReconnectTest() {
        sequenceNumber(
                this::stsBase,
                this::verifyStsBase,
                IteratorBuilder::reconnectRequest,
                WrappedRequest::wrapped,
                ShardIteratorType.AFTER_SEQUENCE_NUMBER);
    }

    @Test
    public void getShardSequenceNumberTest() {
        sequenceNumber(this::gsiBase, this::verifyGsiBase, IteratorBuilder::request, WrappedRequest::wrapped);
    }

    @Test
    public void getShardIteratorReconnectTest() {
        sequenceNumber(
                this::gsiBase,
                this::verifyGsiBase,
                IteratorBuilder::reconnectRequest,
                WrappedRequest::wrapped,
                ShardIteratorType.AFTER_SEQUENCE_NUMBER);
    }

    @Test
    public void subscribeTimestampTest() {
        timeStampTest(this::stsBase, this::verifyStsBase, IteratorBuilder::request, WrappedRequest::wrapped);
    }

    @Test
    public void getShardTimestampTest() {
        timeStampTest(this::gsiBase, this::verifyGsiBase, IteratorBuilder::request, WrappedRequest::wrapped);
    }

    private interface IteratorApply<T> {
        T apply(T base, String sequenceNumber, InitialPositionInStreamExtended initialPositionInStreamExtended);
    }

    private <T, R> void latestTest(
            Supplier<T> supplier,
            Consumer<R> baseVerifier,
            IteratorApply<T> iteratorRequest,
            Function<T, WrappedRequest<R>> toRequest) {
        String sequenceNumber = SentinelCheckpoint.LATEST.name();
        InitialPositionInStreamExtended initialPosition =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
        updateTest(
                supplier,
                baseVerifier,
                iteratorRequest,
                toRequest,
                sequenceNumber,
                initialPosition,
                ShardIteratorType.LATEST,
                null,
                null);
    }

    private <T, R> void trimHorizonTest(
            Supplier<T> supplier,
            Consumer<R> baseVerifier,
            IteratorApply<T> iteratorRequest,
            Function<T, WrappedRequest<R>> toRequest) {
        String sequenceNumber = SentinelCheckpoint.TRIM_HORIZON.name();
        InitialPositionInStreamExtended initialPosition =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
        updateTest(
                supplier,
                baseVerifier,
                iteratorRequest,
                toRequest,
                sequenceNumber,
                initialPosition,
                ShardIteratorType.TRIM_HORIZON,
                null,
                null);
    }

    private <T, R> void sequenceNumber(
            Supplier<T> supplier,
            Consumer<R> baseVerifier,
            IteratorApply<T> iteratorRequest,
            Function<T, WrappedRequest<R>> toRequest) {
        sequenceNumber(supplier, baseVerifier, iteratorRequest, toRequest, ShardIteratorType.AT_SEQUENCE_NUMBER);
    }

    private <T, R> void sequenceNumber(
            Supplier<T> supplier,
            Consumer<R> baseVerifier,
            IteratorApply<T> iteratorRequest,
            Function<T, WrappedRequest<R>> toRequest,
            ShardIteratorType shardIteratorType) {
        InitialPositionInStreamExtended initialPosition =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
        updateTest(
                supplier,
                baseVerifier,
                iteratorRequest,
                toRequest,
                SEQUENCE_NUMBER,
                initialPosition,
                shardIteratorType,
                "1234",
                null);
    }

    private <T, R> void timeStampTest(
            Supplier<T> supplier,
            Consumer<R> baseVerifier,
            IteratorApply<T> iteratorRequest,
            Function<T, WrappedRequest<R>> toRequest) {
        String sequenceNumber = SentinelCheckpoint.AT_TIMESTAMP.name();
        InitialPositionInStreamExtended initialPosition =
                InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(TIMESTAMP.toEpochMilli()));
        updateTest(
                supplier,
                baseVerifier,
                iteratorRequest,
                toRequest,
                sequenceNumber,
                initialPosition,
                ShardIteratorType.AT_TIMESTAMP,
                null,
                TIMESTAMP);
    }

    private <T, R> void updateTest(
            Supplier<T> supplier,
            Consumer<R> baseVerifier,
            IteratorApply<T> iteratorRequest,
            Function<T, WrappedRequest<R>> toRequest,
            String sequenceNumber,
            InitialPositionInStreamExtended initialPositionInStream,
            ShardIteratorType expectedShardIteratorType,
            String expectedSequenceNumber,
            Instant expectedTimestamp) {
        T base = supplier.get();
        T updated = iteratorRequest.apply(base, sequenceNumber, initialPositionInStream);
        WrappedRequest<R> request = toRequest.apply(updated);
        baseVerifier.accept(request.request());
        assertThat(request.shardIteratorType(), equalTo(expectedShardIteratorType));
        assertThat(request.sequenceNumber(), equalTo(expectedSequenceNumber));
        assertThat(request.timestamp(), equalTo(expectedTimestamp));
    }

    private interface WrappedRequest<R> {
        ShardIteratorType shardIteratorType();

        String sequenceNumber();

        Instant timestamp();

        R request();

        static WrappedRequest<SubscribeToShardRequest> wrapped(SubscribeToShardRequest.Builder builder) {
            SubscribeToShardRequest req = builder.build();
            return new WrappedRequest<SubscribeToShardRequest>() {
                @Override
                public ShardIteratorType shardIteratorType() {
                    return req.startingPosition().type();
                }

                @Override
                public String sequenceNumber() {
                    return req.startingPosition().sequenceNumber();
                }

                @Override
                public Instant timestamp() {
                    return req.startingPosition().timestamp();
                }

                @Override
                public SubscribeToShardRequest request() {
                    return req;
                }
            };
        }

        static WrappedRequest<GetShardIteratorRequest> wrapped(GetShardIteratorRequest.Builder builder) {
            GetShardIteratorRequest req = builder.build();
            return new WrappedRequest<GetShardIteratorRequest>() {
                @Override
                public ShardIteratorType shardIteratorType() {
                    return req.shardIteratorType();
                }

                @Override
                public String sequenceNumber() {
                    return req.startingSequenceNumber();
                }

                @Override
                public Instant timestamp() {
                    return req.timestamp();
                }

                @Override
                public GetShardIteratorRequest request() {
                    return req;
                }
            };
        }
    }

    private void verifyStsBase(SubscribeToShardRequest req) {
        assertThat(req.shardId(), equalTo(SHARD_ID));
        assertThat(req.consumerARN(), equalTo(CONSUMER_ARN));
    }

    private void verifyGsiBase(GetShardIteratorRequest req) {
        assertThat(req.streamName(), equalTo(STREAM_NAME));
        assertThat(req.shardId(), equalTo(SHARD_ID));
    }

    private SubscribeToShardRequest.Builder stsBase() {
        return SubscribeToShardRequest.builder().shardId(SHARD_ID).consumerARN(CONSUMER_ARN);
    }

    private GetShardIteratorRequest.Builder gsiBase() {
        return GetShardIteratorRequest.builder().shardId(SHARD_ID).streamName(STREAM_NAME);
    }
}
