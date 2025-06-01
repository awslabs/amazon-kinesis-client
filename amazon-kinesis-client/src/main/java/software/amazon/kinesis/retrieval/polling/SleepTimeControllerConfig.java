package software.amazon.kinesis.retrieval.polling;

import java.time.Instant;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Configuration for the {@link SleepTimeController}.
 */
@KinesisClientInternalApi
@EqualsAndHashCode
@Data
@Accessors(fluent = true)
@Builder
public class SleepTimeControllerConfig {
    private Instant lastSuccessfulCall;
    private long idleMillisBetweenCalls;
    private Integer lastRecordsCount;
    private Long lastMillisBehindLatest;
}
