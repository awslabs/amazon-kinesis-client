package software.amazon.kinesis.retrieval;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.SentinelCheckpoint;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;

@KinesisClientInternalApi
public class IteratorBuilder {

    public static SubscribeToShardRequest.Builder request(
            SubscribeToShardRequest.Builder builder,
            String sequenceNumber,
            InitialPositionInStreamExtended initialPosition) {
        return builder.startingPosition(request(StartingPosition.builder(), sequenceNumber, initialPosition)
                .build());
    }

    public static SubscribeToShardRequest.Builder reconnectRequest(
            SubscribeToShardRequest.Builder builder,
            String sequenceNumber,
            InitialPositionInStreamExtended initialPosition) {
        return builder.startingPosition(reconnectRequest(StartingPosition.builder(), sequenceNumber, initialPosition)
                .build());
    }

    public static StartingPosition.Builder request(
            StartingPosition.Builder builder, String sequenceNumber, InitialPositionInStreamExtended initialPosition) {
        return apply(
                builder,
                StartingPosition.Builder::type,
                StartingPosition.Builder::timestamp,
                StartingPosition.Builder::sequenceNumber,
                initialPosition,
                sequenceNumber,
                ShardIteratorType.AT_SEQUENCE_NUMBER);
    }

    public static StartingPosition.Builder reconnectRequest(
            StartingPosition.Builder builder, String sequenceNumber, InitialPositionInStreamExtended initialPosition) {
        return apply(
                builder,
                StartingPosition.Builder::type,
                StartingPosition.Builder::timestamp,
                StartingPosition.Builder::sequenceNumber,
                initialPosition,
                sequenceNumber,
                ShardIteratorType.AFTER_SEQUENCE_NUMBER);
    }

    /**
     * Creates a GetShardIteratorRequest builder that uses AT_SEQUENCE_NUMBER ShardIteratorType.
     *
     * @param builder         An initial GetShardIteratorRequest builder to be updated.
     * @param sequenceNumber  The sequence number to restart the request from.
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP.
     * @return An updated GetShardIteratorRequest.Builder.
     */
    public static GetShardIteratorRequest.Builder request(
            GetShardIteratorRequest.Builder builder,
            String sequenceNumber,
            InitialPositionInStreamExtended initialPosition) {
        return getShardIteratorRequest(builder, sequenceNumber, initialPosition, ShardIteratorType.AT_SEQUENCE_NUMBER);
    }

    /**
     * Creates a GetShardIteratorRequest builder that uses AFTER_SEQUENCE_NUMBER ShardIteratorType.
     *
     * @param builder         An initial GetShardIteratorRequest builder to be updated.
     * @param sequenceNumber  The sequence number to restart the request from.
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP.
     * @return An updated GetShardIteratorRequest.Builder.
     */
    public static GetShardIteratorRequest.Builder reconnectRequest(
            GetShardIteratorRequest.Builder builder,
            String sequenceNumber,
            InitialPositionInStreamExtended initialPosition) {
        return getShardIteratorRequest(
                builder, sequenceNumber, initialPosition, ShardIteratorType.AFTER_SEQUENCE_NUMBER);
    }

    private static GetShardIteratorRequest.Builder getShardIteratorRequest(
            GetShardIteratorRequest.Builder builder,
            String sequenceNumber,
            InitialPositionInStreamExtended initialPosition,
            ShardIteratorType shardIteratorType) {
        return apply(
                builder,
                GetShardIteratorRequest.Builder::shardIteratorType,
                GetShardIteratorRequest.Builder::timestamp,
                GetShardIteratorRequest.Builder::startingSequenceNumber,
                initialPosition,
                sequenceNumber,
                shardIteratorType);
    }

    private static final Map<String, ShardIteratorType> SHARD_ITERATOR_MAPPING;

    static {
        Map<String, ShardIteratorType> map = new HashMap<>();
        map.put(SentinelCheckpoint.LATEST.name(), ShardIteratorType.LATEST);
        map.put(SentinelCheckpoint.TRIM_HORIZON.name(), ShardIteratorType.TRIM_HORIZON);
        map.put(SentinelCheckpoint.AT_TIMESTAMP.name(), ShardIteratorType.AT_TIMESTAMP);

        SHARD_ITERATOR_MAPPING = Collections.unmodifiableMap(map);
    }

    @FunctionalInterface
    private interface UpdatingFunction<T, R> {
        R apply(R updated, T value);
    }

    private static <R> R apply(
            R initial,
            UpdatingFunction<ShardIteratorType, R> shardIterFunc,
            UpdatingFunction<Instant, R> dateFunc,
            UpdatingFunction<String, R> sequenceFunction,
            InitialPositionInStreamExtended initialPositionInStreamExtended,
            String sequenceNumber,
            ShardIteratorType defaultIteratorType) {
        ShardIteratorType iteratorType = SHARD_ITERATOR_MAPPING.getOrDefault(sequenceNumber, defaultIteratorType);
        R result = shardIterFunc.apply(initial, iteratorType);
        switch (iteratorType) {
            case AT_TIMESTAMP:
                return dateFunc.apply(
                        result, initialPositionInStreamExtended.getTimestamp().toInstant());
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                return sequenceFunction.apply(result, sequenceNumber);
            default:
                return result;
        }
    }
}
